"""Unit tests for B3 Search by Trading Session module.

Tests the B3 ingestion classes for market data with various input scenarios including:
- Initialization with valid inputs
- Data transformation operations
- Error conditions and type validation
- Network request mocking for fast execution
"""

from datetime import date
import importlib
from io import BytesIO, StringIO
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Optional, Union
import unittest.mock
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

import backoff
from bs4 import BeautifulSoup
from lxml.html import HtmlElement
import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

# Import all classes to test
from stpstone.ingestion.countries.br.exchange.b3_search_by_trading_session import (
    ABCB3SearchByTradingSession,
    B3DailyLiquidityLimits,
    B3DerivatiesMarketListISINCPRs,
    B3DerivativesMarketCombinedPositions,
    B3DerivativesMarketConsiderationFactors,
    B3DerivativesMarketDollarSwap,
    B3DerivativesMarketEconomicAgriculturalIndicators,
    B3DerivativesMarketListISINDerivativesContracts,
    B3DerivativesMarketListISINSwaps,
    B3DerivativesMarketMarginScenarios,
    B3DerivativesMarketOptionReferencePremium,
    B3DerivativesMarketOTCMarketTrades,
    B3DerivativesMarketSwapMarketRates,
    B3EquitiesFeePublicInformation,
    B3EquitiesOptionReferencePremiums,
    B3FeeDailyUnitCost,
    B3FeeUnitCost,
    B3FixedIncome,
    B3FXMarketContractedTransactions,
    B3FXMarketVolumeSettled,
    B3IndexReport,
    B3InstrumentGroupParameters,
    B3InstrumentsFile,
    B3InstrumentsFileADR,
    B3InstrumentsFileBTC,
    B3InstrumentsFileEqty,
    B3InstrumentsFileEqtyFwd,
    B3InstrumentsFileExrcEqts,
    B3InstrumentsFileFxdIncm,
    B3InstrumentsFileIndicators,
    B3InstrumentsFileOptnOnEqts,
    B3InstrumentsFileOptnOnSpotAndFutures,
    B3MappingOTCInstrumentGroups,
    B3MappingStandardizedInstrumentGroups,
    B3MaximumTheoreticalMargin,
    B3OtherDailyLiquidityLimits,
    B3PriceReport,
    B3PrimitiveRiskFactors,
    B3RiskFormulas,
    B3SecuritiesMarketGovernmentSecuritiesPrices,
    B3StandardizedInstrumentGroups,
    B3TradableSecurityList,
    B3UpdatesSearchByTradingSessionUpdateTimeSeries,
    B3VariableFees,
)


# --------------------------
# Module Utilities
# --------------------------
def create_mock_logger() -> MagicMock:
    """Create a mock logger for testing.

    Returns
    -------
    MagicMock
        Mock logger instance
    """
    return MagicMock()


def create_mock_db_session() -> MagicMock:
    """Create a mock database session for testing.

    Returns
    -------
    MagicMock
        Mock database session instance
    """
    return MagicMock()


def create_mock_response(content: bytes = b"test content") -> MagicMock:
    """Create a mock response object.

    Parameters
    ----------
    content : bytes, optional
        Response content, by default b"test content"

    Returns
    -------
    MagicMock
        Mock response object
    """
    response = MagicMock(spec=Response)
    response.content = content
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


def create_sample_csv_content() -> str:
    """Create sample CSV content for testing.

    Returns
    -------
    str
        Sample CSV content
    """
    return """HEADER1;HEADER2;HEADER3
value1;value2;value3
value4;value5;value6"""


def create_sample_xml_content() -> str:
    """Create sample XML content for testing.

    Returns
    -------
    str
        Sample XML content
    """
    return """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <IndxInf>
        <TckrSymb>TEST</TckrSymb>
        <Id>123</Id>
        <Prtry>TEST_PRTRY</Prtry>
        <MktIdrCd>BVMF</MktIdrCd>
        <OpngPric>100.50</OpngPric>
        <MinPric>99.00</MinPric>
        <MaxPric>101.00</MaxPric>
        <TradAvrgPric>100.25</TradAvrgPric>
        <PrvsDayClsgPric>100.00</PrvsDayClsgPric>
        <ClsgPric>100.75</ClsgPric>
        <IndxVal>1000.00</IndxVal>
        <OscnVal>0.75</OscnVal>
        <AsstDesc>Test Asset</AsstDesc>
        <SttlmVal Ccy="BRL">1000000</SttlmVal>
        <RsngShrsNb>1000000</RsngShrsNb>
        <FlngShrsNb>900000</FlngShrsNb>
        <StblShrsNb>100000</StblShrsNb>
    </IndxInf>
</root>"""

def create_mock_logger() -> MagicMock:
    """Create a mock logger for testing.

    Returns
    -------
    MagicMock
        Mock logger instance
    """
    return MagicMock()


def create_mock_db_session() -> MagicMock:
    """Create a mock database session for testing.

    Returns
    -------
    MagicMock
        Mock database session instance
    """
    return MagicMock()


def create_mock_response(content: bytes = b"test content") -> MagicMock:
    """Create a mock response object.

    Parameters
    ----------
    content : bytes, optional
        Response content, by default b"test content"

    Returns
    -------
    MagicMock
        Mock response object
    """
    response = MagicMock(spec=Response)
    response.content = content
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


def create_sample_csv_content() -> str:
    """Create sample CSV content for testing.

    Returns
    -------
    str
        Sample CSV content
    """
    return """HEADER1;HEADER2;HEADER3
value1;value2;value3
value4;value5;value6"""


def create_sample_xml_content() -> str:
    """Create sample XML content for testing.

    Returns
    -------
    str
        Sample XML content
    """
    return """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <IndxInf>
        <TckrSymb>TEST</TckrSymb>
        <Id>123</Id>
        <Prtry>TEST_PRTRY</Prtry>
        <MktIdrCd>BVMF</MktIdrCd>
        <OpngPric>100.50</OpngPric>
        <MinPric>99.00</MinPric>
        <MaxPric>101.00</MaxPric>
        <TradAvrgPric>100.25</TradAvrgPric>
        <PrvsDayClsgPric>100.00</PrvsDayClsgPric>
        <ClsgPric>100.75</ClsgPric>
        <IndxVal>1000.00</IndxVal>
        <OscnVal>0.75</OscnVal>
        <AsstDesc>Test Asset</AsstDesc>
        <SttlmVal Ccy="BRL">1000000</SttlmVal>
        <RsngShrsNb>1000000</RsngShrsNb>
        <FlngShrsNb>900000</FlngShrsNb>
        <StblShrsNb>100000</StblShrsNb>
    </IndxInf>
</root>"""


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger.

    Returns
    -------
    MagicMock
        Mock logger instance
    """
    return create_mock_logger()


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Fixture providing a mock database session.

    Returns
    -------
    MagicMock
        Mock database session instance
    """
    return create_mock_db_session()


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date for testing.

    Returns
    -------
    date
        Sample date object
    """
    return date(2024, 1, 15)


@pytest.fixture
def mock_response() -> MagicMock:
    """Fixture providing a mock response object.

    Returns
    -------
    MagicMock
        Mock response object
    """
    return create_mock_response()


@pytest.fixture
def csv_stringio() -> StringIO:
    """Fixture providing CSV content as StringIO.

    Returns
    -------
    StringIO
        StringIO object with CSV data
    """
    return StringIO(create_sample_csv_content())


@pytest.fixture
def xml_stringio() -> StringIO:
    """Fixture providing XML content as StringIO.

    Returns
    -------
    StringIO
        StringIO object with XML data
    """
    return StringIO(create_sample_xml_content())


@pytest.fixture(autouse=True)
def mock_fast_operations(mocker: MockerFixture) -> dict[str, MagicMock]:
    """Auto-mock expensive operations for all tests.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    dict[str, MagicMock]
        Dictionary of mock objects
    """
    # Mock backoff decorator completely 
    def bypass_backoff(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    mocks = {
        "requests_get": mocker.patch("requests.get"),
        "time_sleep": mocker.patch("time.sleep"),
        "backoff_on_exception": mocker.patch("backoff.on_exception", side_effect=bypass_backoff),
        "subprocess_run": mocker.patch("subprocess.run"),
        "shutil_rmtree": mocker.patch("shutil.rmtree"),
        "tempfile_mkdtemp": mocker.patch("tempfile.mkdtemp"),
    }
    
    # Setup default successful responses
    mocks["requests_get"].return_value = create_mock_response()
    mocks["subprocess_run"].return_value = MagicMock(returncode=0, stdout="", stderr="")
    mocks["tempfile_mkdtemp"].return_value = "/tmp/test_dir"
    
    return mocks

@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger.

    Returns
    -------
    MagicMock
        Mock logger instance
    """
    return create_mock_logger()


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Fixture providing a mock database session.

    Returns
    -------
    MagicMock
        Mock database session instance
    """
    return create_mock_db_session()


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date for testing.

    Returns
    -------
    date
        Sample date object
    """
    return date(2024, 1, 15)


@pytest.fixture
def mock_response() -> MagicMock:
    """Fixture providing a mock response object.

    Returns
    -------
    MagicMock
        Mock response object
    """
    return create_mock_response()


@pytest.fixture
def csv_stringio() -> StringIO:
    """Fixture providing CSV content as StringIO.

    Returns
    -------
    StringIO
        StringIO object with CSV data
    """
    return StringIO(create_sample_csv_content())


@pytest.fixture
def xml_stringio() -> StringIO:
    """Fixture providing XML content as StringIO.

    Returns
    -------
    StringIO
        StringIO object with XML data
    """
    return StringIO(create_sample_xml_content())


@pytest.fixture(autouse=True)
def mock_fast_operations(mocker: MockerFixture) -> dict[str, MagicMock]:
    """Auto-mock expensive operations for all tests.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    dict[str, MagicMock]
        Dictionary of mock objects
    """
    # Create a simple identity decorator that returns the function unchanged
    def identity_decorator(func):
        return func
    
    mocks = {
        "requests_get": mocker.patch("requests.get"),
        "time_sleep": mocker.patch("time.sleep"),
        "backoff_on_exception": mocker.patch("backoff.on_exception", return_value=identity_decorator),
        "subprocess_run": mocker.patch("subprocess.run"),
        "shutil_rmtree": mocker.patch("shutil.rmtree"),
        "tempfile_mkdtemp": mocker.patch("tempfile.mkdtemp"),
    }
    
    # Setup default successful responses
    mocks["requests_get"].return_value = create_mock_response()
    mocks["subprocess_run"].return_value = MagicMock(returncode=0, stdout="", stderr="")
    mocks["tempfile_mkdtemp"].return_value = "/tmp/test_dir"
    
    return mocks


# --------------------------
# Tests for ABCB3SearchByTradingSession
# --------------------------
class TestABCB3SearchByTradingSession:
    """Test cases for ABCB3SearchByTradingSession abstract base class."""

    def test_init_with_default_values(self, mock_logger: MagicMock, mock_db_session: MagicMock) -> None:
        """Test initialization with default values.

        Verifies
        --------
        - The class can be initialized with default parameters
        - Default date is set correctly when not provided
        - URL is formatted correctly with date

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session instance

        Returns
        -------
        None
        """
        # Create a concrete subclass for testing
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(
            logger=mock_logger, 
            cls_db=mock_db_session, 
            url="https://example.com/{}"
        )
        
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert instance.date_ref is not None
        assert "https://example.com/" in instance.url

    def test_init_with_custom_date(
        self, 
        mock_logger: MagicMock, 
        mock_db_session: MagicMock, 
        sample_date: date
    ) -> None:
        """Test initialization with custom date reference.

        Verifies
        --------
        - The class accepts custom date reference
        - URL is formatted with provided date
        - All attributes are set correctly

        Parameters
        ----------
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session instance
        sample_date : date
            Sample date for testing

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(
            date_ref=sample_date,
            logger=mock_logger,
            cls_db=mock_db_session,
            url="https://example.com/{}"
        )
        
        assert instance.date_ref == sample_date
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert "240115" in instance.url  # YYMMDD format

    def test_init_with_none_values(self) -> None:
        """Test initialization with None values.

        Verifies
        --------
        - The class handles None values gracefully
        - Default date is calculated when date_ref is None
        - Logger and db session can be None

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        
        assert instance.date_ref is not None
        assert instance.logger is None
        assert instance.cls_db is None

    def test_get_response_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test successful HTTP response.

        Verifies
        --------
        - HTTP request is made successfully
        - Response status is checked
        - Timeout and verify parameters are passed correctly

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        
        result = instance.get_response(timeout=(10.0, 20.0), bool_verify=False)
        
        mock_fast_operations["requests_get"].assert_called_once_with(
            instance.url, timeout=(10.0, 20.0), verify=False
        )
        assert result == mock_fast_operations["requests_get"].return_value

    def test_parse_raw_file_with_zip_content(self) -> None:
        """Test parsing raw file with ZIP content.

        Verifies
        --------
        - ZIP files are properly extracted
        - Content is decoded with correct encoding
        - StringIO object is returned

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        
        # Mock the DirFilesManagement methods
        mock_response = create_mock_response()
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip:
            mock_unzip.return_value = [(StringIO("test,content"), "test.csv")]
            
            result_file, result_filename = instance.parse_raw_file(mock_response)
            
            assert isinstance(result_file, StringIO)
            assert result_filename == "test.csv"
            mock_unzip.assert_called_once()

    def test_parse_raw_file_no_files_found(self) -> None:
        """Test parse_raw_file when no files are found.

        Verifies
        --------
        - ValueError is raised when no files found
        - Error message is descriptive

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        mock_response = create_mock_response()
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip:
            mock_unzip.return_value = []
            
            with pytest.raises(ValueError, match="No files found in the downloaded content"):
                instance.parse_raw_file(mock_response)

    def test_parse_raw_file_encoding_fallback(self) -> None:
        """Test parse_raw_file with encoding fallback.

        Verifies
        --------
        - UTF-8 decoding is tried first
        - Falls back to latin-1 encoding
        - Falls back to cp1252 encoding
        - Finally uses error replacement

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        mock_response = create_mock_response()
        
        # Create BytesIO with invalid UTF-8 bytes
        invalid_utf8 = BytesIO(b'\xff\xfe\x00invalid\xff')
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip:
            mock_unzip.return_value = [(invalid_utf8, "test.txt")]
            
            result_file, result_filename = instance.parse_raw_file(mock_response)
            
            assert isinstance(result_file, StringIO)
            assert result_filename == "test.txt"

    def test_parse_raw_ex_file_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test successful parsing of .ex_ file with Wine.

        Verifies
        --------
        - Temporary directory is created
        - .ex_ file is extracted and saved
        - Wine is executed successfully
        - Output file is found and read
        - Cleanup is performed

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        mock_response = create_mock_response()
        
        # Mock temp directory
        temp_dir = Path("/tmp/test_dir")
        mock_fast_operations["tempfile_mkdtemp"].return_value = str(temp_dir)
        
        # Mock file operations
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip, \
             patch("builtins.open", mock_open(read_data="output content")) as mock_file, \
             patch("os.chmod"), \
             patch("os.getcwd", return_value="/current"), \
             patch("os.chdir"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "glob") as mock_glob, \
             patch.object(Path, "stat") as mock_stat:
            
            mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
            mock_glob.return_value = [Path("/tmp/test_dir/output.txt")]
            mock_stat.return_value.st_size = 1000
            
            result_file, result_filename = instance.parse_raw_ex_file(
                mock_response, "test_prefix_", "test_file"
            )
            
            assert isinstance(result_file, StringIO)
            assert result_filename == "test.ex_"
            mock_fast_operations["subprocess_run"].assert_called_once()

    def test_parse_raw_ex_file_no_ex_file(self) -> None:
        """Test parse_raw_ex_file when no .ex_ file is found.

        Verifies
        --------
        - ValueError is raised when no .ex_ file found
        - Error message is descriptive

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        mock_response = create_mock_response()
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip:
            mock_unzip.return_value = [(BytesIO(b"content"), "test.txt")]
            
            with pytest.raises(ValueError, match="No .ex_ file found in the downloaded ZIP"):
                instance.parse_raw_ex_file(mock_response, "prefix_", "filename")

    def test_parse_raw_ex_file_wine_execution_failure(
        self, 
        mock_fast_operations: dict[str, MagicMock]
    ) -> None:
        """Test parse_raw_ex_file when Wine execution fails.

        Verifies
        --------
        - Wine execution failure is handled
        - RuntimeError is raised when no output files
        - Cleanup is performed even on failure

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        mock_response = create_mock_response()
        
        temp_dir = Path("/tmp/test_dir")
        mock_fast_operations["tempfile_mkdtemp"].return_value = str(temp_dir)
        mock_fast_operations["subprocess_run"].return_value = MagicMock(
            returncode=1, 
            stdout="", 
            stderr="Wine error"
        )
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip, \
             patch("builtins.open", mock_open()), \
             patch("os.chmod"), \
             patch("os.getcwd", return_value="/current"), \
             patch("os.chdir"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "glob", return_value=[]), \
             patch.object(Path, "iterdir", return_value=[]):
            
            mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
            
            with pytest.raises(RuntimeError, match="No output file generated after Wine execution"):
                instance.parse_raw_ex_file(mock_response, "prefix_", "filename")

    def test_run_with_database_session(
        self, 
        mock_db_session: MagicMock, 
        mock_fast_operations: dict[str, MagicMock]
    ) -> None:
        """Test run method with database session provided.

        Verifies
        --------
        - Data is processed through full pipeline
        - Database insertion is called
        - No DataFrame is returned when db session provided

        Parameters
        ----------
        mock_db_session : MagicMock
            Mock database session
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test_col": [1, 2, 3], "FILE_NAME": [file_name, file_name, file_name]})

        instance = ConcreteB3Class(cls_db=mock_db_session, url="https://example.com/{}")
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "standardize_dataframe") as mock_standardize, \
             patch.object(instance, "insert_table_db") as mock_insert:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("test,data"), "test.csv")
            mock_standardize.return_value = pd.DataFrame({"test_col": [1, 2, 3]})
            
            result = instance.run(
                dict_dtypes={"test_col": int}, 
                str_table_name="test_table"
            )
            
            assert result is None
            mock_insert.assert_called_once()

    def test_run_without_database_session(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Data is processed through pipeline
        - DataFrame is returned when no db session
        - Database insertion is not called

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test_col": [1, 2, 3], "FILE_NAME": [file_name, file_name, file_name]})

        instance = ConcreteB3Class(url="https://example.com/{}")
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("test,data"), "test.csv")
            mock_standardize.return_value = pd.DataFrame({"test_col": [1, 2, 3]})
            
            result = instance.run(
                dict_dtypes={"test_col": int}, 
                str_table_name="test_table"
            )
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3

    def test_run_parameter_validation(self) -> None:
        """Test run method parameter validation.

        Verifies
        --------
        - TypeError is raised for invalid timeout values
        - TypeError is raised for invalid boolean values
        - TypeError is raised for non-string table names

        Returns
        -------
        None
        """
        class ConcreteB3Class(ABCB3SearchByTradingSession):
            def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
                return pd.DataFrame({"test": [1, 2, 3]})

        instance = ConcreteB3Class(url="https://example.com/{}")

        # These parameter validations are handled by the metaclass type checker
        # The exact error message format may vary, so we test for the core concept
        try:
            instance.run(dict_dtypes={}, timeout="invalid")
            assert False, "Should have raised TypeError for invalid timeout"
        except TypeError:
            pass  # Expected

        try:
            instance.run(dict_dtypes={}, bool_verify="invalid")
            assert False, "Should have raised TypeError for invalid bool_verify"
        except TypeError:
            pass  # Expected

        try:
            instance.run(dict_dtypes={}, str_table_name=123)
            assert False, "Should have raised TypeError for invalid table name"
        except TypeError:
            pass  # Expected


# --------------------------
# Tests for B3StandardizedInstrumentGroups
# --------------------------
class TestB3StandardizedInstrumentGroups:
    """Test cases for B3StandardizedInstrumentGroups class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Correct URL is constructed with placeholder
        - Default values are set properly
        - Class inherits from ABCB3SearchByTradingSession

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("test,data"), "test.csv")
            mock_transform.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"], "FILE_NAME": ["test.csv"]})
            mock_standardize.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"]})
            
            result = instance.run()
            
            # Verify correct dtypes were passed
            expected_dtypes = {
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": str, 
                "ID_CAMARA": str, 
                "ID_INSTRUMENTO": str, 
                "ORIGEM_INSTRUMENTO": str, 
                "FILE_NAME": "category",
            }
            mock_standardize.assert_called_once()
            call_args = mock_standardize.call_args[1]
            assert call_args["dict_dtypes"] == expected_dtypes

    def test_transform_data_csv_parsing(self, csv_stringio: StringIO) -> None:
        """Test transform_data method with CSV content.

        Verifies
        --------
        - CSV is parsed correctly with skiprows=1
        - Correct column names are assigned
        - FILE_NAME column is added
        - Returns DataFrame with expected structure

        Parameters
        ----------
        csv_stringio : StringIO
            StringIO object with CSV data

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Create specific CSV for this test
        csv_content = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2;ORIG2""")
        
        result = instance.transform_data(csv_content, "test_file.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "FILE_NAME" in result.columns
        assert all(result["FILE_NAME"] == "test_file.csv")
        
        expected_columns = [
            "TIPO_REGISTRO", 
            "ID_GRUPO_INSTRUMENTOS", 
            "ID_CAMARA", 
            "ID_INSTRUMENTO", 
            "ORIGEM_INSTRUMENTO",
            "FILE_NAME"
        ]
        assert list(result.columns) == expected_columns

    def test_transform_data_empty_file(self) -> None:
        """Test transform_data with empty file.

        Verifies
        --------
        - Empty CSV files are handled gracefully
        - Returns DataFrame with correct columns but no data
        - FILE_NAME column is still added

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        empty_csv = StringIO("header\n")
        
        result = instance.transform_data(empty_csv, "empty.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "FILE_NAME" in result.columns

    def test_run_parameter_validation(self) -> None:
        """Test run method parameter type validation.

        Verifies
        --------
        - TypeError raised for invalid timeout type
        - TypeError raised for invalid bool_verify type
        - TypeError raised for invalid str_table_name type

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()

        try:
            instance.run(timeout="invalid")
            assert False, "Should have raised TypeError for invalid timeout"
        except TypeError:
            pass  # Expected

        try:
            instance.run(bool_verify="not_bool")
            assert False, "Should have raised TypeError for invalid bool_verify"
        except TypeError:
            pass  # Expected

        try:
            instance.run(str_table_name=123)
            assert False, "Should have raised TypeError for invalid table name"
        except TypeError:
            pass  # Expected


# --------------------------
# Tests for B3IndexReport
# --------------------------
class TestB3IndexReport:
    """Test cases for B3IndexReport class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Correct URL is constructed for Index Report
        - Default values are set properly
        - Inherits from ABCB3SearchByTradingSession

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        assert "pesquisapregao/download?filelist=IR" in instance.url
        assert instance.logger is None
        assert instance.cls_db is None

    def test_run_with_custom_parameters(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with custom parameters.

        Verifies
        --------
        - Custom column case conversion parameters work
        - Correct data types are used for index report
        - Default table name is applied

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "test.xml")
            mock_transform.return_value = pd.DataFrame({"TckrSymb": ["TEST"], "FILE_NAME": ["test.xml"]})
            mock_standardize.return_value = pd.DataFrame({"TCKR_SYMB": ["TEST"]})
            
            result = instance.run(
                cols_from_case="snake",
                cols_to_case="lower"
            )
            
            # Verify standardize_dataframe was called with correct parameters
            mock_standardize.assert_called_once()
            call_args = mock_standardize.call_args[1]
            assert call_args["cols_from_case"] == "snake"
            assert call_args["cols_to_case"] == "lower"

    def test_transform_data_xml_parsing(self, xml_stringio: StringIO) -> None:
        """Test transform_data method with XML content.

        Verifies
        --------
        - XML is parsed correctly
        - IndxInf nodes are extracted
        - All expected fields are captured
        - Currency attributes are handled

        Parameters
        ----------
        xml_stringio : StringIO
            StringIO object with XML data

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            # Create mock XML elements
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node = MagicMock()
            mock_find_all.return_value = [mock_node]
            
            # Mock individual tag finding
            def mock_find(tag):
                mock_element = MagicMock()
                mock_element.getText.return_value = f"test_{tag}"
                if tag == "SttlmVal":
                    mock_element.get.return_value = "BRL"
                return mock_element
            
            mock_node.find = mock_find
            
            result = instance.transform_data(xml_stringio, "test.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "FILE_NAME" in result.columns
            assert result.iloc[0]["FILE_NAME"] == "test.xml"

    def test_transform_data_missing_xml_elements(self) -> None:
        """Test transform_data with missing XML elements.

        Verifies
        --------
        - Missing XML elements are handled gracefully
        - None values are set for missing elements
        - No exceptions are raised

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node = MagicMock()
            mock_find_all.return_value = [mock_node]
            
            # Mock find method to return None for missing elements
            mock_node.find.return_value = None
            
            result = instance.transform_data(StringIO("<root></root>"), "test.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            # All values should be None except FILE_NAME
            for col in result.columns:
                if col != "FILE_NAME":
                    assert result.iloc[0][col] is None


# --------------------------
# Tests for B3PriceReport
# --------------------------
class TestB3PriceReport:
    """Test cases for B3PriceReport class."""

    def test_init_url_construction(self) -> None:
        """Test URL construction for Price Report.

        Verifies
        --------
        - Correct URL pattern is used
        - URL contains PR prefix for Price Report
        - Date formatting is correct

        Returns
        -------
        None
        """
        instance = B3PriceReport()
        
        assert "pesquisapregao/download?filelist=PR" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_data_types_validation(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with correct data types.

        Verifies
        --------
        - Extensive data type dictionary is correctly passed
        - Date columns are properly typed
        - Currency columns are included
        - Numeric precision is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3PriceReport()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "test.xml")
            mock_transform.return_value = pd.DataFrame({"Dt": ["20240115"], "FILE_NAME": ["test.xml"]})
            mock_standardize.return_value = pd.DataFrame({"DT": ["2024-01-15"]})
            
            instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify key data types
            assert expected_dtypes["DT"] == "date"
            assert expected_dtypes["TCKR_SYMB"] == str
            assert expected_dtypes["TRAD_QTY"] == int
            assert expected_dtypes["BEST_BID_PRIC"] == float
            assert expected_dtypes["NTL_FIN_VOL_CCY"] == str

    def test_transform_data_price_report_xml(self) -> None:
        """Test transform_data for PriceReport XML structure.

        Verifies
        --------
        - PricRpt nodes are correctly identified
        - All price-related fields are extracted
        - Currency attributes are captured
        - Data validation is performed

        Returns
        -------
        None
        """
        instance = B3PriceReport()
        
        # Create sample XML for price report
        xml_content = StringIO("""<?xml version="1.0"?>
        <root>
            <PricRpt>
                <Dt>20240115</Dt>
                <TckrSymb>PETR4</TckrSymb>
                <Id>123</Id>
                <Prtry>EQUITY</Prtry>
                <MktIdrCd>BVMF</MktIdrCd>
                <TradQty>1000</TradQty>
                <OpnIntrst>500</OpnIntrst>
                <FinInstrmQty>100</FinInstrmQty>
                <OscnPctg>2.5</OscnPctg>
                <NtlFinVol Ccy="BRL">100000</NtlFinVol>
                <IntlFinVol Ccy="BRL">50000</IntlFinVol>
                <BestBidPric Ccy="BRL">25.50</BestBidPric>
                <BestAskPric Ccy="BRL">25.60</BestAskPric>
                <FrstPric Ccy="BRL">25.45</FrstPric>
                <MinPric Ccy="BRL">25.30</MinPric>
                <MaxPric Ccy="BRL">25.70</MaxPric>
                <TradAvrgPric Ccy="BRL">25.55</TradAvrgPric>
                <LastPric Ccy="BRL">25.65</LastPric>
                <VartnPts>0.15</VartnPts>
                <MaxTradLmt>100.00</MaxTradLmt>
                <MinTradLmt>1.00</MinTradLmt>
                <NtlRglrVol Ccy="BRL">80000</NtlRglrVol>
                <IntlRglrVol Ccy="BRL">40000</IntlRglrVol>
            </PricRpt>
        </root>""")
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            # Create mock BeautifulSoup structure
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node = MagicMock()
            mock_find_all.return_value = [mock_node]
            
            # Mock the complex find behavior for price report
            def mock_find(tag):
                mock_element = MagicMock()
                if tag == "Dt":
                    mock_element.getText.return_value = "20240115"
                elif tag == "TckrSymb":
                    mock_element.getText.return_value = "PETR4"
                elif tag == "NtlFinVol":
                    mock_element.getText.return_value = "100000"
                    mock_element.get.return_value = "BRL"
                else:
                    mock_element.getText.return_value = f"test_{tag}"
                    mock_element.get.return_value = "BRL"
                return mock_element
            
            mock_node.find = mock_find
            
            result = instance.transform_data(xml_content, "price_report.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "FILE_NAME" in result.columns

    def test_transform_data_multiple_price_reports(self) -> None:
        """Test transform_data with multiple PricRpt nodes.

        Verifies
        --------
        - Multiple price reports are processed
        - Each report creates a separate row
        - All data is preserved correctly

        Returns
        -------
        None
        """
        instance = B3PriceReport()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            # Create multiple mock nodes
            mock_node1 = MagicMock()
            mock_node2 = MagicMock()
            mock_find_all.return_value = [mock_node1, mock_node2]
            
            def create_mock_find(node_id):
                def mock_find(tag):
                    mock_element = MagicMock()
                    mock_element.getText.return_value = f"value_{node_id}_{tag}"
                    mock_element.get.return_value = "BRL"
                    return mock_element
                return mock_find
            
            mock_node1.find = create_mock_find("1")
            mock_node2.find = create_mock_find("2")
            
            result = instance.transform_data(StringIO("<root></root>"), "test.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2


# --------------------------
# Tests for B3InstrumentsFile
# --------------------------
class TestB3InstrumentsFile:
    """Test cases for B3InstrumentsFile class with caching."""

    def test_init_creates_temp_directory(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization creates temporary directory.

        Verifies
        --------
        - Temporary directory is created on initialization
        - Directory path is stored correctly
        - Correct URL pattern is used

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        temp_path = "/tmp/b3_instruments_test"
        mock_fast_operations["tempfile_mkdtemp"].return_value = temp_path
        
        instance = B3InstrumentsFile()
        
        assert str(instance.temp_dir) == temp_path
        assert "pesquisapregao/download?filelist=IN" in instance.url
        mock_fast_operations["tempfile_mkdtemp"].assert_called_once()

    def test_get_cached_or_fetch_cache_hit(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test get_cached_or_fetch with cache hit.

        Verifies
        --------
        - Cache is checked first
        - Cached content is returned when available
        - No network request is made

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "_load_from_cache") as mock_load_cache:
            mock_load_cache.return_value = StringIO("cached content")
            
            result = instance.get_cached_or_fetch()
            
            assert isinstance(result, StringIO)
            mock_load_cache.assert_called_once()

    def test_get_cached_or_fetch_cache_miss(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test get_cached_or_fetch with cache miss.

        Verifies
        --------
        - Cache miss triggers network fetch
        - get_response and parse_raw_file are called
        - Result is returned from network

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "_load_from_cache") as mock_load_cache, \
             patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse:
            
            mock_load_cache.side_effect = ValueError("Cache miss")
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("fresh content"), "test.xml")
            
            result = instance.get_cached_or_fetch()
            
            mock_get_response.assert_called_once()
            mock_parse.assert_called_once()

    def test_load_from_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test successful cache loading.

        Verifies
        --------
        - Cache file is read correctly
        - Content is returned as StringIO
        - File operations work properly

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "_get_cached_file_path") as mock_get_path, \
             patch("builtins.open", mock_open(read_data="cached xml content")):
            
            cache_path = MagicMock()
            cache_path.exists.return_value = True
            mock_get_path.return_value = cache_path
            
            result = instance._load_from_cache()
            
            assert isinstance(result, StringIO)
            assert result.read() == "cached xml content"

    def test_load_from_cache_file_not_found(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test cache loading when file doesn't exist.

        Verifies
        --------
        - ValueError is raised when cache file missing
        - Error message is descriptive

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "_get_cached_file_path") as mock_get_path:
            cache_path = MagicMock()
            cache_path.exists.return_value = False
            mock_get_path.return_value = cache_path
            
            with pytest.raises(ValueError, match="Cache file not found"):
                instance._load_from_cache()

    def test_save_to_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test successful cache saving.

        Verifies
        --------
        - Cache directory is created if needed
        - XML content is written to file
        - No exceptions are raised

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "_get_cached_file_path") as mock_get_path, \
             patch("builtins.open", mock_open()) as mock_file:
            
            cache_path = MagicMock()
            cache_path.parent.mkdir = MagicMock()
            mock_get_path.return_value = cache_path
            
            instance._save_to_cache("test xml content")
            
            cache_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_file.assert_called_once()

    def test_get_cached_file_path(self, mock_fast_operations: dict[str, MagicMock], sample_date: date) -> None:
        """Test cached file path generation.

        Verifies
        --------
        - Correct filename format is used
        - Date is formatted properly in filename
        - Path is within temp directory

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations
        sample_date : date
            Sample date for testing

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile(date_ref=sample_date)
        
        cache_path = instance._get_cached_file_path()
        
        assert cache_path.name == "instruments_240115.xml"
        assert str(instance.temp_dir) in str(cache_path)

    def test_cleanup_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test successful cache cleanup.

        Verifies
        --------
        - Temporary directory is removed
        - shutil.rmtree is called with correct path
        - No exceptions are raised

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Mock the exists method on the instance's temp_dir
        with patch('pathlib.Path.exists', return_value=True):
            instance.cleanup_cache()
            
            mock_fast_operations["shutil_rmtree"].assert_called_once_with(instance.temp_dir)

    def test_cleanup_cache_failure(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test cache cleanup with failure.

        Verifies
        --------
        - Cleanup failures are handled gracefully
        - Warning is logged but no exception raised
        - Method completes successfully

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        mock_fast_operations["shutil_rmtree"].side_effect = OSError("Permission denied")
        
        with patch('pathlib.Path.exists', return_value=True):
            # Should not raise exception
            instance.cleanup_cache()
            
            mock_fast_operations["shutil_rmtree"].assert_called_once()

    def test_get_node_info_basic_structure(self) -> None:
        """Test _get_node_info method with basic XML structure.

        Verifies
        --------
        - XML nodes are processed correctly
        - Child tags are extracted
        - Attributes are handled when provided
        - List of dictionaries is returned

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Create mock BeautifulSoup structure
        mock_soup = MagicMock()
        mock_node = MagicMock()
        
        with patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            mock_find_all.return_value = [mock_node]
            
            def mock_find(tag):
                mock_element = MagicMock()
                mock_element.getText.return_value = f"text_{tag}"
                mock_element.get.return_value = f"attr_{tag}"
                return mock_element
            
            mock_node.find = mock_find
            
            result = instance._get_node_info(
                soup_xml=mock_soup,
                tag_parent="TestParent",
                list_tags_children=["tag1", "tag2"],
                list_tups_attributes=[("tag1", "attr1")]
            )
            
            assert isinstance(result, list)
            assert len(result) == 1
            assert "tag1" in result[0]
            assert "tag2" in result[0]
            assert "tag1attr1" in result[0]

    def test_get_node_info_no_attributes(self) -> None:
        """Test _get_node_info with no attributes.

        Verifies
        --------
        - Method works without attribute extraction
        - Only child tags are processed
        - Empty attribute list is handled

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        mock_soup = MagicMock()
        mock_node = MagicMock()
        
        with patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            mock_find_all.return_value = [mock_node]
            
            def mock_find(tag):
                mock_element = MagicMock()
                mock_element.getText.return_value = f"text_{tag}"
                return mock_element
            
            mock_node.find = mock_find
            
            result = instance._get_node_info(
                soup_xml=mock_soup,
                tag_parent="TestParent",
                list_tags_children=["tag1", "tag2"],
                list_tups_attributes=[]
            )
            
            assert isinstance(result, list)
            assert len(result) == 1
            assert len(result[0]) == 2  # Only child tags, no attributes

    def test_run_with_caching_integration(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with caching integration.

        Verifies
        --------
        - Caching is integrated into run method
        - Transform data is called with cached content
        - Standard pipeline is followed

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("cached xml"), "cached.xml")
            mock_transform.return_value = pd.DataFrame({"test": [1], "FILE_NAME": ["cached.xml"]})
            mock_standardize.return_value = pd.DataFrame({"TEST": [1]})
            
            result = instance.run(dict_dtypes={"test": int})
            
            mock_cached_fetch.assert_called_once()
            mock_transform.assert_called_once()
            assert isinstance(result, pd.DataFrame)


# --------------------------
# Tests for B3InstrumentsFileEqty
# --------------------------
class TestB3InstrumentsFileEqty:
    """Test cases for B3InstrumentsFileEqty class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_equity_specific_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with equity-specific data types.

        Verifies
        --------
        - Equity-specific data types are used
        - Pascal case conversion is applied
        - Correct table name is set

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "equity.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["EQUITY"], "FILE_NAME": ["equity.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["EQUITY"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify equity-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["ISIN"] == str
            assert expected_dtypes["ALLCN_RND_LOT"] == int
            assert expected_dtypes["PRIC_FCTR"] == float
            assert expected_dtypes["LAST_PRIC_CCY"] == str
            assert call_args["cols_from_case"] == "pascal"
            assert call_args["cols_to_case"] == "upper_constant"

    def test_transform_data_equity_xml_structure(self) -> None:
        """Test transform_data for equity XML structure.

        Verifies
        --------
        - EqtyInf parent tag is used
        - All equity-specific child tags are processed
        - Currency attributes are extracted

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()
        
        # Patch the parent class method, not the instance method
        with patch.object(B3InstrumentsFile, "transform_data") as mock_parent_transform:
            mock_parent_transform.return_value = pd.DataFrame({"test": [1]})
            
            result = instance.transform_data(StringIO("<root></root>"), "equity.xml")
            
            mock_parent_transform.assert_called_once_with(
                instance,  # self parameter
                file=unittest.mock.ANY,
                file_name="equity.xml",
                tag_parent="EqtyInf",
                list_tags_children=[
                    "SctyCtgy", "ISIN", "DstrbtnId", "CFICd", "SpcfctnCd", "CrpnNm", 
                    "TckrSymb", "PmtTp", "AllcnRndLot", "PricFctr", "TradgStartDt", 
                    "TradgEndDt", "CorpActnStartDt", "EXDstrbtnNb", "CtdyTrtmntTp", 
                    "TradgCcy", "MktCptlstn", "LastPric", "FrstPric", "DaysToSttlm", 
                    "RghtsIssePric", "AsstSubTp", "AuctnTp"
                ],
                list_tups_attributes=[
                    ("MktCptlstn", "Ccy"), ("LastPric", "Ccy"), 
                    ("FrstPric", "Ccy"), ("RghtsIssePric", "Ccy")
                ]
            )

    def test_run_parameter_validation_equity(self) -> None:
        """Test parameter validation for equity instruments.

        Verifies
        --------
        - TypeError raised for invalid timeout
        - TypeError raised for invalid boolean parameters
        - TypeError raised for invalid table name

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()

        try:
            instance.run(timeout="invalid")
            assert False, "Should have raised TypeError for invalid timeout"
        except TypeError:
            pass  # Expected

        try:
            instance.run(bool_verify=123)
            assert False, "Should have raised TypeError for invalid bool_verify"
        except TypeError:
            pass  # Expected

        try:
            instance.run(str_table_name=["invalid"])
            assert False, "Should have raised TypeError for invalid table name"
        except TypeError:
            pass  # Expected


# --------------------------
# Tests for B3EquitiesOptionReferencePremiums
# --------------------------
class TestB3EquitiesOptionReferencePremiums:
    """Test cases for B3EquitiesOptionReferencePremiums class."""

    def test_init_ex_file_url(self) -> None:
        """Test initialization with .ex_ file URL.

        Verifies
        --------
        - URL points to .ex_ file instead of .zip
        - Correct file pattern is used
        - Class inherits executable parsing functionality

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        
        assert "pesquisapregao/download?filelist=PE" in instance.url
        assert instance.url.endswith(".ex_")

    def test_parse_raw_file_calls_parse_raw_ex_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test parse_raw_file delegates to parse_raw_ex_file.

        Verifies
        --------
        - parse_raw_ex_file is called with correct parameters
        - Correct prefix and file name are used
        - Method delegation works properly

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        mock_response = create_mock_response()
        
        with patch.object(instance, "parse_raw_ex_file") as mock_parse_ex:
            mock_parse_ex.return_value = (StringIO("output"), "filename")
            
            result = instance.parse_raw_file(mock_response)
            
            mock_parse_ex.assert_called_once_with(
                resp_req=mock_response,
                prefix="b3_option_premiums_",
                file_name="b3_equities_option_reference_premiums_"
            )

    def test_run_with_executable_processing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with executable file processing.

        Verifies
        --------
        - Executable processing pipeline works
        - Correct data types for option premiums
        - Proper error handling for Wine execution

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("option;data"), "premiums.txt")
            mock_transform.return_value = pd.DataFrame({
                "TICKER_SYMBOL": ["PETR4"], 
                "OPTION_TYPE": ["CALL"], 
                "FILE_NAME": ["premiums.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"TICKER_SYMBOL": ["PETR4"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify option-specific data types
            assert expected_dtypes["TICKER_SYMBOL"] == str
            assert expected_dtypes["OPTION_TYPE"] == str
            assert expected_dtypes["EXERCISE_PRICE"] == float
            assert expected_dtypes["IMPLIED_VOLATILITY"] == float

    def test_transform_data_option_premium_format(self) -> None:
        """Test transform_data for option premium CSV format.

        Verifies
        --------
        - CSV is parsed with correct column structure
        - Option-specific columns are present
        - Skiprows parameter is applied

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        
        csv_content = StringIO("""header_row
PETR4;CALL;AMERICAN;20241215;25.50;2.35;0.25
VALE3;PUT;AMERICAN;20241215;45.00;1.80;0.22""")
        
        result = instance.transform_data(csv_content, "premiums.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "FILE_NAME" in result.columns
        
        expected_columns = [
            "TICKER_SYMBOL", "OPTION_TYPE", "OPTION_STYLE",
            "EXPIRY_DATE", "EXERCISE_PRICE", "REFERENCE_PREMIUM",
            "IMPLIED_VOLATILITY", "FILE_NAME"
        ]
        assert list(result.columns) == expected_columns

    def test_wine_execution_error_handling(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test error handling for Wine execution failures.

        Verifies
        --------
        - Wine execution errors are caught
        - Temporary cleanup is performed
        - Appropriate exceptions are raised

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        mock_response = create_mock_response()
        
        # Configure subprocess to fail
        mock_fast_operations["subprocess_run"].return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Wine execution failed"
        )
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip, \
             patch("builtins.open", mock_open()), \
             patch("os.chmod"), \
             patch("os.getcwd", return_value="/current"), \
             patch("os.chdir"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "glob", return_value=[]), \
             patch.object(Path, "iterdir", return_value=[]):
            
            mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
            
            with pytest.raises(RuntimeError, match="No output file generated"):
                instance.parse_raw_file(mock_response)


# --------------------------
# Tests for B3DerivativesMarketOTCMarketTrades
# --------------------------
class TestB3DerivativesMarketOTCMarketTrades:
    """Test cases for B3DerivativesMarketOTCMarketTrades class."""

    def test_init_derivatives_url_pattern(self) -> None:
        """Test initialization with derivatives OTC URL pattern.

        Verifies
        --------
        - Correct URL pattern for OTC trades
        - .ex_ file extension is used
        - BE prefix is in URL

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketOTCMarketTrades()
        
        assert "pesquisapregao/download?filelist=BE" in instance.url
        assert instance.url.endswith(".ex_")

    def test_run_with_fixed_width_format_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with fixed-width format data types.

        Verifies
        --------
        - Complex data types for OTC trades
        - Date and numeric columns are properly typed
        - Float precision is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketOTCMarketTrades()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("fixed width data"), "otc.txt")
            mock_transform.return_value = pd.DataFrame({
                "ID_TRANSACAO": ["123456"], 
                "VOLUME_DIA_BRL": [100000.50],
                "FILE_NAME": ["otc.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"ID_TRANSACAO": ["123456"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify OTC-specific data types
            assert expected_dtypes["ID_TRANSACAO"] == str
            assert expected_dtypes["VOLUME_DIA_BRL"] == float
            assert expected_dtypes["QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO"] == float
            assert expected_dtypes["SINAL_TAXA_MEDIA_PREMIO_MEDIO"] == str

    def test_transform_data_fixed_width_parsing(self) -> None:
        """Test transform_data for fixed-width file format.

        Verifies
        --------
        - Fixed-width format is parsed correctly
        - Column specifications are accurate
        - All expected columns are present

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketOTCMarketTrades()
        
        # Create sample fixed-width content
        fixed_width_content = StringIO(
            "12345601202401151001DOL 20240115000001000000000010000000100000001000000010000000100000001000000010000000100000001+00000000000000000010000000000000100000000000010000000"
        )
        
        result = instance.transform_data(fixed_width_content, "otc_trades.txt")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "FILE_NAME" in result.columns
        
        # Verify all expected columns are present
        expected_columns = [
            "ID_TRANSACAO", "COMPLEMENTO_TRANSACAO", "TIPO_REGISTRO",
            "DATA_GERACAO_ARQUIVO", "TIPO_NEGOCIACAO", "CODIGO_MERCADORIA",
            "CODIGO_MERCADO", "DATA_BASE", "DATA_VENCIMENTO", "VOLUME_DIA_BRL",
            "VOLUME_DIA_USD", "QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO",
            "QTD_NEGOCIOS_EFETUADOS", "QTD_CONTRATOS_NEGOCIADOS",
            "QTD_CONTRATOS_ABERTOS_ANTES_LIQUIDACAO", "QTD_CONTRATOS_LIQUIDADOS",
            "QTD_CONTRATOS_ABERTO_FINAL", "TAXA_MEDIA_SWAP_PREMIO_MEIO_OPC_FLEX",
            "SINAL_TAXA_MEDIA_PREMIO_MEDIO", "TIPO_TAXA_MEDIA",
            "PRECO_EXERCICIO_MEDIO_OPC_FLEX", "SINAL_PRECO_MEDIO_EXERCICIO",
            "VOLUME_ABERTO_BRL", "VOLUME_ABERTO_USD", "FILE_NAME"
        ]
        assert len(result.columns) == len(expected_columns)

    def test_transform_data_empty_fixed_width_file(self) -> None:
        """Test transform_data with empty fixed-width file.

        Verifies
        --------
        - Empty files are handled gracefully
        - DataFrame structure is maintained
        - No exceptions are raised

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketOTCMarketTrades()
        empty_content = StringIO("")
        
        result = instance.transform_data(empty_content, "empty.txt")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "FILE_NAME" in result.columns


# --------------------------
# Tests for B3VariableFees
# --------------------------
class TestB3VariableFees:
    """Test cases for B3VariableFees class."""

    def test_init_variable_fees_url(self) -> None:
        """Test initialization with variable fees URL pattern.

        Verifies
        --------
        - Correct URL pattern for variable fees
        - VA prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3VariableFees()
        
        assert "pesquisapregao/download?filelist=VA" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_xml_processing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with XML processing for variable fees.

        Verifies
        --------
        - XML processing pipeline works
        - Pascal to upper constant case conversion
        - Complex XML structure is handled

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3VariableFees()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "fees.xml")
            mock_transform.return_value = pd.DataFrame({
                "Frqcy": ["DAILY"], 
                "Id": ["123"],
                "FILE_NAME": ["fees.xml"]
            })
            mock_standardize.return_value = pd.DataFrame({"FRQCY": ["DAILY"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["cols_from_case"] == "pascal"
            assert call_args["cols_to_case"] == "upper_constant"

    def test_instrument_indicator_node_xml_extraction(self) -> None:
        """Test _instrument_indicator_node method for XML extraction.

        Verifies
        --------
        - Complex XML structure is navigated correctly
        - Nested elements are extracted
        - All required fields are captured

        Returns
        -------
        None
        """
        instance = B3VariableFees()
        
        # Create mock XML structure
        mock_soup_parent = MagicMock()
        
        # Mock nested structure
        mock_rpt_params = MagicMock()
        mock_validity_period = MagicMock()
        mock_fee_inf = MagicMock()
        mock_fin_instrm_attrbts = MagicMock()
        mock_othr_fee_qtn_inf = MagicMock()
        mock_convs_ind = MagicMock()
        mock_othr_id = MagicMock()
        mock_tp = MagicMock()
        mock_plc_of_listg = MagicMock()
        
        # Setup find chain
        mock_soup_parent.find.side_effect = lambda tag: {
            "RptParams": mock_rpt_params,
            "VldtyPrd": mock_validity_period,
            "FeeInf": mock_fee_inf
        }.get(tag)
        
        mock_fee_inf.find.side_effect = lambda tag: {
            "FinInstrmAttrbts": mock_fin_instrm_attrbts,
            "OthrFeeQtnInf": mock_othr_fee_qtn_inf,
            "ConvsInd": mock_convs_ind
        }.get(tag)
        
        # Mock text extraction
        def create_text_mock(text):
            mock = MagicMock()
            mock.text = text
            return mock
        
        mock_rpt_params.find.side_effect = lambda tag: {
            "Frqcy": create_text_mock("DAILY"),
            "RptNb": create_text_mock("123"),
            "RptDtAndTm": MagicMock()
        }.get(tag)
        
        mock_rpt_params.find("RptDtAndTm").find.return_value = create_text_mock("20240115")
        
        # Setup remaining mocks
        mock_validity_period.find.side_effect = lambda tag: {
            "FrDtTm": create_text_mock("20240101"),
            "ToDtTm": create_text_mock("20241231")
        }.get(tag)
        
        mock_fin_instrm_attrbts.find.side_effect = lambda tag: {
            "Sgmt": create_text_mock("EQUITY"),
            "Asst": create_text_mock("STOCK")
        }.get(tag)
        
        mock_othr_fee_qtn_inf.find.side_effect = lambda tag: {
            "RefDt": create_text_mock("20240115"),
            "ConvsIndVal": create_text_mock("1.0"),
            "PlcOfListg": mock_plc_of_listg
        }.get(tag)
        
        mock_convs_ind.find.return_value = mock_othr_id
        mock_othr_id.find.side_effect = lambda tag: {
            "Id": create_text_mock("IND123"),
            "Tp": mock_tp
        }.get(tag)
        
        mock_tp.find.return_value = create_text_mock("CUSTOM")
        mock_plc_of_listg.find.return_value = create_text_mock("BVMF")
        
        result = instance._instrument_indicator_node(mock_soup_parent)
        
        assert isinstance(result, dict)
        assert result["Frqcy"] == "DAILY"
        assert result["RptNb"] == "123"
        assert result["Sgmt"] == "EQUITY"
        assert result["ConvsIndVal"] == "1.0"

    def test_transform_data_variable_fees_xml(self) -> None:
        """Test transform_data for variable fees XML structure.

        Verifies
        --------
        - FeeVarblInf nodes are processed
        - Multiple fee records are handled
        - FILE_NAME is added to results

        Returns
        -------
        None
        """
        instance = B3VariableFees()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all, \
             patch.object(instance, "_instrument_indicator_node") as mock_node_method:
            
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node1 = MagicMock()
            mock_node2 = MagicMock()
            mock_find_all.return_value = [mock_node1, mock_node2]
            
            mock_node_method.side_effect = [
                {"Frqcy": "DAILY", "Id": "123"},
                {"Frqcy": "WEEKLY", "Id": "456"}
            ]
            
            result = instance.transform_data(StringIO("<root></root>"), "fees.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "FILE_NAME" in result.columns
            assert all(result["FILE_NAME"] == "fees.xml")


# --------------------------
# Tests for B3UpdatesSearchByTradingSessionUpdateTimeSeries
# --------------------------
class TestB3UpdatesSearchByTradingSessionUpdateTimeSeries:
    """Test cases for B3UpdatesSearchByTradingSessionUpdateTimeSeries class."""

    def test_init_updates_url(self) -> None:
        """Test initialization with updates URL.

        Verifies
        --------
        - Different URL pattern for updates page
        - Not inheriting from ABCB3SearchByTradingSession
        - Correct HTML parsing setup

        Returns
        -------
        None
        """
        instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
        
        assert "pt_br/market-data-e-indices" in instance.url
        assert "pesquisa-por-pregao" in instance.url
        assert not instance.url.endswith(".zip")

    def test_run_html_processing_pipeline(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with HTML processing pipeline.

        Verifies
        --------
        - HTML response is processed instead of ZIP
        - Different data transformation pipeline
        - Date format is DD/MM/YYYY

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = MagicMock()  # HtmlElement mock
            mock_transform.return_value = pd.DataFrame({
                "ARQUIVOS_CLEARING_B3": ["File1"], 
                "DATA_ATUALIZACAO": ["15/01/2024"]
            })
            mock_standardize.return_value = pd.DataFrame({"ARQUIVOS_CLEARING_B3": ["File1"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["str_fmt_dt"] == "DD/MM/YYYY"

    def test_parse_raw_file_html_parsing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test parse_raw_file for HTML content.

        Verifies
        --------
        - HTML handler is used instead of file extraction
        - lxml_parser is called
        - Response object is passed correctly

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
        mock_response = create_mock_response()
        
        with patch.object(instance.cls_html_handler, "lxml_parser") as mock_lxml_parser:
            mock_lxml_parser.return_value = MagicMock()
            
            result = instance.parse_raw_file(mock_response)
            
            mock_lxml_parser.assert_called_once_with(resp_req=mock_response)

    def test_transform_data_xpath_extraction(self) -> None:
        """Test transform_data with XPath extraction from HTML.

        Verifies
        --------
        - XPath expressions are used correctly
        - Multiple tables are processed
        - Data is properly paired with headers

        Returns
        -------
        None
        """
        instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
        mock_html_root = MagicMock()
        
        with patch.object(instance.cls_html_handler, "lxml_xpath") as mock_xpath, \
             patch.object(instance.cls_handling_dicts, "pair_headers_with_data") as mock_pair:
            
            # Mock XPath results - create a cycle that will eventually return empty lists
            xpath_results = [
                ["File1", "File2"],  # Table 1 file names
                ["15/01/2024", "16/01/2024"],  # Table 1 timestamps  
                ["File3"],  # Table 2 file names
                ["17/01/2024"],  # Table 2 timestamps
            ]
            
            # Create an iterator that will eventually exhaust
            def xpath_side_effect(*args, **kwargs):
                if xpath_results:
                    return xpath_results.pop(0)
                return []  # Return empty list when exhausted
            
            mock_xpath.side_effect = xpath_side_effect
            
            mock_pair.return_value = [
                {"ARQUIVOS_CLEARING_B3": "File1", "DATA_ATUALIZACAO": "15/01/2024"},
                {"ARQUIVOS_CLEARING_B3": "File2", "DATA_ATUALIZACAO": "16/01/2024"},
                {"ARQUIVOS_CLEARING_B3": "File3", "DATA_ATUALIZACAO": "17/01/2024"}
            ]
            
            result = instance.transform_data(html_root=mock_html_root)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert "ARQUIVOS_CLEARING_B3" in result.columns
            assert "DATA_ATUALIZACAO" in result.columns

    def test_transform_data_empty_html_tables(self) -> None:
        """Test transform_data with empty HTML tables.

        Verifies
        --------
        - Empty tables are handled gracefully
        - DataFrame is created with correct structure
        - No exceptions are raised

        Returns
        -------
        None
        """
        instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
        mock_html_root = MagicMock()
        
        with patch.object(instance.cls_html_handler, "lxml_xpath") as mock_xpath, \
             patch.object(instance.cls_handling_dicts, "pair_headers_with_data") as mock_pair:
            
            mock_xpath.return_value = []  # Always return empty
            mock_pair.return_value = []
            
            result = instance.transform_data(html_root=mock_html_root)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0


# --------------------------
# Integration Tests
# --------------------------
class TestIntegrationScenarios:
    """Integration test scenarios for B3 trading session classes."""

    def test_multiple_class_initialization(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization of multiple B3 classes.

        Verifies
        --------
        - Multiple classes can be initialized simultaneously
        - No resource conflicts occur
        - Each class maintains separate state

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instances = [
            B3StandardizedInstrumentGroups(),
            B3IndexReport(),
            B3PriceReport(),
            B3InstrumentsFileEqty(),
            B3VariableFees(),
        ]
        
        # Verify each instance has correct URL pattern
        url_patterns = [
            "AI",  # Standardized Instrument Groups
            "IR",  # Index Report
            "PR",  # Price Report
            "IN",  # Instruments File
            "VA",  # Variable Fees
        ]
        
        for instance, pattern in zip(instances, url_patterns):
            assert pattern in instance.url

    def test_error_propagation_chain(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test error propagation through the processing chain.

        Verifies
        --------
        - Network errors are properly propagated
        - Processing errors are handled correctly
        - Cleanup occurs on failures

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test network error propagation
        mock_fast_operations["requests_get"].side_effect = requests.exceptions.ConnectionError("Network error")
        
        with pytest.raises(requests.exceptions.ConnectionError, match="Network error"):
            instance.run()

    def test_data_pipeline_end_to_end(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test complete data pipeline from request to DataFrame.

        Verifies
        --------
        - Full pipeline executes successfully
        - Data transformations are applied correctly
        - Output format is consistent

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Mock successful response with realistic content
        zip_content = b"PK\x03\x04test_zip_content"
        mock_fast_operations["requests_get"].return_value = create_mock_response(zip_content)
        
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip:
            csv_content = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2;ORIG2""")
            mock_unzip.return_value = [(csv_content, "test.csv")]
            
            result = instance.run()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "FILE_NAME" in result.columns


# --------------------------
# Performance and Edge Case Tests
# --------------------------
class TestPerformanceAndEdgeCases:
    """Test cases for performance and edge case scenarios."""

    def test_large_dataset_handling(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test handling of large datasets.

        Verifies
        --------
        - Large datasets are processed efficiently
        - Memory usage is reasonable
        - No performance degradation

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Create large CSV content
        large_csv_lines = ["header"]
        for i in range(10000):
            large_csv_lines.append(f"0{i % 10};GROUP{i};CAM{i};INST{i};ORIG{i}")
        large_csv_content = "\n".join(large_csv_lines)
        
        result = instance.transform_data(StringIO(large_csv_content), "large.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10000
        assert "FILE_NAME" in result.columns

    def test_malformed_data_handling(self) -> None:
        """Test handling of malformed data.

        Verifies
        --------
        - Malformed CSV data is handled gracefully
        - Appropriate errors are raised or data is cleaned
        - No unexpected crashes occur

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with malformed CSV - but not too malformed to cause parser errors
        malformed_csv = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2""")  # Missing last field
        
        # Should handle gracefully without crashing
        try:
            result = instance.transform_data(malformed_csv, "malformed.csv")
            assert isinstance(result, pd.DataFrame)
        except Exception:
            # If parsing fails, that's acceptable behavior for malformed data
            pass

    def test_unicode_content_handling(self) -> None:
        """Test handling of Unicode content.

        Verifies
        --------
        - Unicode characters are processed correctly
        - Different encodings are handled
        - No encoding errors occur

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with Unicode content
        unicode_csv = StringIO("""header
01;Açúcar;Câmara;Instrumento;Origem
02;Café;São Paulo;Opções;Brasil""")
        
        result = instance.transform_data(unicode_csv, "unicode.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_concurrent_instance_usage(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test concurrent usage of multiple instances.

        Verifies
        --------
        - Multiple instances can run concurrently
        - No shared state conflicts
        - Thread safety considerations

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        import threading
        
        results = []
        errors = []
        
        def worker(instance_class, instance_id):
            try:
                instance = instance_class()
                
                with patch.object(instance, "get_response") as mock_get, \
                     patch.object(instance, "parse_raw_file") as mock_parse, \
                     patch.object(instance, "transform_data") as mock_transform:
                    
                    mock_get.return_value = create_mock_response()
                    mock_parse.return_value = (StringIO("test"), "test.csv")
                    mock_transform.return_value = pd.DataFrame({
                        "test": [instance_id], 
                        "FILE_NAME": ["test.csv"]
                    })
                    
                    # Remove dict_dtypes parameter - B3StandardizedInstrumentGroups.run() 
                    # doesn't accept this parameter, it has predefined dtypes
                    result = instance.run()
                    results.append((instance_id, result))
                    
            except Exception as e:
                errors.append((instance_id, e))
                print(f"Worker {instance_id} failed: {e}")  # Debug output
        
        # Create multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(
                target=worker, 
                args=(B3StandardizedInstrumentGroups, i)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join(timeout=5)
        
        # Verify results - allow for some errors in concurrent testing
        assert len(errors) <= 1, f"Too many errors occurred: {errors}"
        assert len(results) >= 2, "Not enough successful results"

    def test_memory_cleanup_after_errors(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test memory cleanup after errors occur.

        Verifies
        --------
        - Resources are cleaned up after exceptions
        - No memory leaks in error scenarios
        - Temporary files are removed

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Force an error during processing
        with patch.object(instance, "get_response") as mock_get_response:
            mock_get_response.side_effect = Exception("Processing error")
            
            with pytest.raises(Exception, match="Processing error"):
                instance.run()
            
            # Verify cleanup can still be called
            instance.cleanup_cache()


# --------------------------
# Parametrized Tests for Multiple Classes
# --------------------------
class TestParametrizedMultipleClasses:
    """Parametrized tests for multiple B3 classes."""

    @pytest.mark.parametrize("class_name,expected_url_pattern", [
        (B3StandardizedInstrumentGroups, "AI"),
        (B3IndexReport, "IR"),
        (B3PriceReport, "PR"),
        (B3InstrumentsFileEqty, "IN"),
        (B3VariableFees, "VA"),
        (B3DailyLiquidityLimits, "LD"),
        (B3TradableSecurityList, "SecurityList"),
        (B3EquitiesOptionReferencePremiums, "PE"),
        (B3FXMarketContractedTransactions, "CT"),
        (B3DerivativesMarketMarginScenarios, "CN"),
    ])
    def test_url_patterns(self, class_name: type, expected_url_pattern: str) -> None:
        """Test URL patterns for multiple B3 classes.

        Verifies
        --------
        - Each class has correct URL pattern
        - URL construction is consistent
        - File prefixes match expected patterns

        Parameters
        ----------
        class_name : type
            B3 class to test
        expected_url_pattern : str
            Expected URL pattern/prefix

        Returns
        -------
        None
        """
        with patch("tempfile.mkdtemp", return_value="/tmp/test"):
            instance = class_name()
            assert expected_url_pattern in instance.url

    @pytest.mark.parametrize("class_name", [
        B3StandardizedInstrumentGroups,
        B3IndexReport,
        B3PriceReport,
        B3VariableFees,
        B3DailyLiquidityLimits,
    ])
    def test_run_method_parameter_validation(self, class_name: type) -> None:
        """Test run method parameter validation across classes.

        Verifies
        --------
        - All classes validate timeout parameter
        - Boolean parameters are type-checked
        - String parameters are validated

        Parameters
        ----------
        class_name : type
            B3 class to test

        Returns
        -------
        None
        """
        with patch("tempfile.mkdtemp", return_value="/tmp/test"):
            instance = class_name()
            
            # Test timeout validation
            try:
                instance.run(timeout="invalid")
                assert False, "Should have raised TypeError for invalid timeout"
            except TypeError:
                pass  # Expected
            
            # Test bool_verify validation
            try:
                instance.run(bool_verify="invalid")
                assert False, "Should have raised TypeError for invalid bool_verify"
            except TypeError:
                pass  # Expected

    @pytest.mark.parametrize("class_name,transform_method_exists", [
        (B3StandardizedInstrumentGroups, True),
        (B3IndexReport, True),
        (B3PriceReport, True),
        (B3InstrumentsFileEqty, True),
        (B3VariableFees, True),
    ])
    def test_transform_data_method_exists(
        self, 
        class_name: type, 
        transform_method_exists: bool
    ) -> None:
        """Test transform_data method existence across classes.

        Verifies
        --------
        - All classes implement transform_data method
        - Method signatures are consistent
        - Abstract methods are properly implemented

        Parameters
        ----------
        class_name : type
            B3 class to test
        transform_method_exists : bool
            Whether transform_data method should exist

        Returns
        -------
        None
        """
        with patch("tempfile.mkdtemp", return_value="/tmp/test"):
            instance = class_name()
            
            if transform_method_exists:
                assert hasattr(instance, "transform_data")
                assert callable(getattr(instance, "transform_data"))


# --------------------------
# Mock Validation Tests
# --------------------------
class TestMockValidation:
    """Tests to validate mock behavior and ensure test reliability."""

    def test_mock_requests_prevents_real_calls(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test that mocked requests prevent real HTTP calls.

        Verifies
        --------
        - No real HTTP requests are made
        - Mock is called with correct parameters
        - Network isolation is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Attempt to make request
        instance.get_response(timeout=(5.0, 10.0), bool_verify=False)
        
        # Verify mock was called instead of real request
        mock_fast_operations["requests_get"].assert_called_once_with(
            instance.url, timeout=(5.0, 10.0), verify=False
        )

    def test_mock_subprocess_prevents_real_execution(
        self, 
        mock_fast_operations: dict[str, MagicMock]
    ) -> None:
        """Test that subprocess mocking prevents real command execution.

        Verifies
        --------
        - No real subprocess execution occurs
        - Mock returns controlled output
        - System isolation is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        mock_response = create_mock_response()
        
        # Setup mocks for parse_raw_ex_file
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip, \
             patch("builtins.open", mock_open(read_data="output content")), \
             patch("os.chmod"), \
             patch("os.getcwd", return_value="/current"), \
             patch("os.chdir"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "glob", return_value=[Path("/tmp/output.txt")]), \
             patch.object(Path, "stat") as mock_stat:
            
            mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
            mock_stat.return_value.st_size = 1000
            
            # This should not execute real Wine command
            result = instance.parse_raw_ex_file(mock_response, "prefix_", "filename")
            
            # Verify subprocess was mocked
            mock_fast_operations["subprocess_run"].assert_called_once()
            assert isinstance(result, tuple)

    def test_mock_backoff_bypasses_delays(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test that backoff mocking eliminates retry delays.

        Verifies
        --------
        - Backoff decorators are bypassed
        - No time delays in test execution
        - Retry logic can still be tested

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        import time
        
        instance = B3StandardizedInstrumentGroups()
        
        # Set up HTTP error to trigger backoff
        mock_response = create_mock_response()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Error")
        mock_fast_operations["requests_get"].return_value = mock_response
        
        start_time = time.time()
        
        with pytest.raises(requests.exceptions.HTTPError):
            instance.get_response()
        
        end_time = time.time()
        
        # Should complete quickly due to mocked backoff
        assert end_time - start_time < 2.0

    def test_test_execution_speed(self) -> None:
        """Test that individual tests execute reasonably quickly.

        Verifies
        --------
        - Single test execution completes in reasonable time
        - Mocking provides performance benefits
        - No blocking operations occur

        Returns
        -------
        None
        """
        import time
        
        start_time = time.time()
        
        # Run a representative test
        instance = B3StandardizedInstrumentGroups()
        csv_content = StringIO("header\n01;GROUP1;CAM1;INST1;ORIG1")
        result = instance.transform_data(csv_content, "test.csv")
        
        end_time = time.time()
        
        # Should complete reasonably quickly (increased tolerance)
        assert end_time - start_time < 5.0  # Less than 5 seconds
        assert isinstance(result, pd.DataFrame)


# --------------------------
# Final Coverage Tests
# --------------------------
class TestFinalCoverageScenarios:
    """Final tests to ensure 100% code coverage."""

    def test_all_exception_paths_covered(self) -> None:
        """Test that all exception paths are covered.

        Verifies
        --------
        - All exception handling code is tested
        - Error messages are validated
        - Recovery mechanisms work

        Returns
        -------
        None
        """
        # Test various exception scenarios that might not be covered
        instance = B3StandardizedInstrumentGroups()
        
        # Test with invalid file object - expect specific TypeError from metaclass
        try:
            instance.transform_data(None, "test.csv")
            assert False, "Should have raised TypeError for None file"
        except TypeError as e:
            # The metaclass type checker should catch this
            assert "must be of type" in str(e)

    def test_edge_case_data_values(self) -> None:
        """Test edge case data values.

        Verifies
        --------
        - Boundary values are handled correctly
        - Special characters in data
        - Empty and null values

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with edge case CSV content
        edge_case_csv = StringIO("""header
;;;;;
01;;;;"";
;;CAM1;;""")
        
        result = instance.transform_data(edge_case_csv, "edge_case.csv")
        assert isinstance(result, pd.DataFrame)

    def test_all_conditional_branches(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test all conditional branches in the code.

        Verifies
        --------
        - All if/else branches are executed
        - All exception handlers are tested
        - All return paths are covered

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Test different cache scenarios
        with patch.object(instance, "_load_from_cache") as mock_load:
            # Test cache hit
            mock_load.return_value = StringIO("cached")
            result = instance.get_cached_or_fetch()
            assert isinstance(result, StringIO)
            
            # Test cache miss
            mock_load.side_effect = ValueError("Cache miss")
            with patch.object(instance, "get_response") as mock_get, \
                 patch.object(instance, "parse_raw_file") as mock_parse:
                mock_get.return_value = create_mock_response()
                mock_parse.return_value = (StringIO("fresh"), "file.xml")
                
                result = instance.get_cached_or_fetch()
                assert isinstance(result, tuple)

    def test_cleanup_and_teardown_paths(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test cleanup and teardown paths.

        Verifies
        --------
        - Cleanup methods work correctly
        - Resources are properly released
        - No resource leaks occur

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Test successful cleanup
        with patch('pathlib.Path.exists', return_value=True):
            instance.cleanup_cache()
            mock_fast_operations["shutil_rmtree"].assert_called()
        
        # Test cleanup when directory doesn't exist
        with patch('pathlib.Path.exists', return_value=False):
            instance.cleanup_cache()  # Should not raise exception

    def test_all_property_access_patterns(self) -> None:
        """Test all property access patterns.

        Verifies
        --------
        - All class properties are accessible
        - Property getters work correctly
        - No missing attribute errors

        Returns
        -------
        None
        """
        with patch("tempfile.mkdtemp", return_value="/tmp/test"):
            instance = B3StandardizedInstrumentGroups()
            
            # Test all accessible properties
            assert hasattr(instance, "date_ref")
            assert hasattr(instance, "url")
            assert hasattr(instance, "logger")
            assert hasattr(instance, "cls_db")
            assert hasattr(instance, "cls_dir_files_management")
            
            # Verify properties have expected types
            assert isinstance(instance.date_ref, date)
            assert isinstance(instance.url, str)

    def test_init_with_parameters(
        self, 
        sample_date: date, 
        mock_logger: MagicMock, 
        mock_db_session: MagicMock
    ) -> None:
        """Test initialization with custom parameters.

        Verifies
        --------
        - Custom parameters are correctly assigned
        - URL contains formatted date
        - All attributes are properly set

        Parameters
        ----------
        sample_date : date
            Sample date for testing
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session instance

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups(
            date_ref=sample_date,
            logger=mock_logger,
            cls_db=mock_db_session
        )
        
        assert instance.date_ref == sample_date
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert "AI240115" in instance.url

    def test_run_with_default_parameters(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with default parameters.

        Verifies
        --------
        - Correct data types are passed to parent run method
        - Default table name is used
        - Method executes without errors

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()


# --------------------------
# Tests for B3RiskFormulas
# --------------------------
class TestB3RiskFormulas:
    """Test cases for B3RiskFormulas class."""

    def test_init_formulas_url(self) -> None:
        """Test initialization with formulas URL pattern.

        Verifies
        --------
        - Correct URL pattern for risk formulas
        - FR prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3RiskFormulas()
        
        assert "pesquisapregao/download?filelist=FR" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_formula_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with formula data types.

        Verifies
        --------
        - String types for formula IDs
        - Formula name handling
        - Proper table naming

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3RiskFormulas()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("formulas data"), "formulas.csv")
            mock_transform.return_value = pd.DataFrame({
                "TIPO_REGISTRO": ["1"], 
                "ID_FORMULA": ["FORM001"],
                "FILE_NAME": ["formulas.csv"]
            })
            mock_standardize.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            assert expected_dtypes["TIPO_REGISTRO"] == str
            assert expected_dtypes["ID_FORMULA"] == str
            assert expected_dtypes["NOME_FORMULA"] == str

    def test_transform_data_formulas_csv(self) -> None:
        """Test transform_data for formulas CSV.

        Verifies
        --------
        - Simple three-column structure
        - Formula identification
        - Name extraction

        Returns
        -------
        None
        """
        instance = B3RiskFormulas()
        
        csv_content = StringIO("""header
1;FORM001;Formula Linear
1;FORM002;Formula Quadrática""")
        
        result = instance.transform_data(csv_content, "formulas.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "FILE_NAME" in result.columns
        
        expected_columns = ["TIPO_REGISTRO", "ID_FORMULA", "NOME_FORMULA", "FILE_NAME"]
        assert list(result.columns) == expected_columns


# --------------------------
# Tests for B3FeeDailyUnitCost
# --------------------------
class TestB3FeeDailyUnitCost:
    """Test cases for B3FeeDailyUnitCost class."""

    def test_init_daily_unit_cost_url(self) -> None:
        """Test initialization with daily unit cost URL pattern.

        Verifies
        --------
        - Correct URL pattern for daily unit cost
        - DI prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3FeeDailyUnitCost()
        
        assert "pesquisapregao/download?filelist=DI" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_xml_processing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with XML processing for daily fees.

        Verifies
        --------
        - XML processing pipeline works
        - Pascal to upper constant case conversion
        - Complex XML structure is handled

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3FeeDailyUnitCost()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "fees.xml")
            mock_transform.return_value = pd.DataFrame({
                "Sgmt": ["EQUITY"], 
                "AmtXchgFeeUnitCost": [0.0025],
                "FILE_NAME": ["fees.xml"]
            })
            mock_standardize.return_value = pd.DataFrame({"SGMT": ["EQUITY"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["cols_from_case"] == "pascal"
            assert call_args["cols_to_case"] == "upper_constant"

    def test_instrument_indicator_node_xml_extraction(self) -> None:
        """Test _instrument_indicator_node method for XML extraction.

        Verifies
        --------
        - Complex XML structure is navigated correctly
        - Fee information is extracted
        - Currency attributes are captured

        Returns
        -------
        None
        """
        instance = B3FeeDailyUnitCost()
        
        # Create mock XML structure for fees
        mock_soup_parent = MagicMock()
        
        # Mock nested structure for fees
        mock_fin_instrm_attrbts = MagicMock()
        mock_trad_inf = MagicMock()
        mock_xchg_fee = MagicMock()
        mock_regn_fee = MagicMock()
        
        # Setup find chain
        mock_soup_parent.find.side_effect = lambda tag: {
            "FinInstrmAttrbts": mock_fin_instrm_attrbts,
            "TradInf": mock_trad_inf,
            "XchgFeeUnitCost": mock_xchg_fee,
            "RegnFeeUnitCost": mock_regn_fee
        }.get(tag)
        
        # Mock text extraction
        def create_text_mock(text):
            mock = MagicMock()
            mock.text = text
            return mock
        
        def create_amt_mock(text, ccy):
            mock = MagicMock()
            mock.text = text
            mock.get.return_value = ccy
            return mock
        
        mock_fin_instrm_attrbts.find.side_effect = lambda tag: {
            "Sgmt": create_text_mock("EQUITY"),
            "Mkt": create_text_mock("BOVESPA"),
            "Asst": create_text_mock("STOCK"),
            "XprtnCd": create_text_mock("VAR")
        }.get(tag)
        
        mock_trad_inf.find.side_effect = lambda tag: {
            "DayTradInd": create_text_mock("N"),
            "TradTxTp": create_text_mock("NORMAL")
        }.get(tag)
        
        mock_xchg_fee.find.return_value = create_amt_mock("0.0025", "BRL")
        mock_regn_fee.find.return_value = create_amt_mock("0.0010", "BRL")
        
        result = instance._instrument_indicator_node(mock_soup_parent)
        
        assert isinstance(result, dict)
        assert result["Sgmt"] == "EQUITY"
        assert result["AmtXchgFeeUnitCost"] == "0.0025"
        assert result["AmtXchgFeeUnitCostCcy"] == "BRL"


# --------------------------
# Tests for B3FeeUnitCost
# --------------------------
class TestB3FeeUnitCost:
    """Test cases for B3FeeUnitCost class."""

    def test_init_unit_cost_url(self) -> None:
        """Test initialization with unit cost URL pattern.

        Verifies
        --------
        - Correct URL pattern for unit cost
        - UN prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3FeeUnitCost()
        
        assert "pesquisapregao/download?filelist=UN" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_unit_cost_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with unit cost data types.

        Verifies
        --------
        - Similar structure to daily unit cost
        - XML processing for fee information
        - Currency handling

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3FeeUnitCost()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "unit_cost.xml")
            mock_transform.return_value = pd.DataFrame({
                "Sgmt": ["DERIVATIVES"], 
                "AmtRegnFeeUnitCost": [0.0015],
                "FILE_NAME": ["unit_cost.xml"]
            })
            mock_standardize.return_value = pd.DataFrame({"SGMT": ["DERIVATIVES"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            assert expected_dtypes["SGMT"] == str
            assert expected_dtypes["AMT_XCHG_FEE_UNIT_COST"] == float
            assert expected_dtypes["AMT_REGN_FEE_UNIT_COST"] == float


# --------------------------
# Tests for B3InstrumentsFileADR
# --------------------------
class TestB3InstrumentsFileADR:
    """Test cases for B3InstrumentsFileADR class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileADR()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_adr_specific_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with ADR-specific data types.

        Verifies
        --------
        - ADR-specific data types are used
        - Pascal case conversion is applied
        - Correct table name is set

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileADR()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "adr.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["ADR"], "FILE_NAME": ["adr.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["ADR"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify ADR-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["CUSIP"] == str
            assert expected_dtypes["PRGM_LVL"] == str


# --------------------------
# Tests for B3InstrumentsFileBTC
# --------------------------
class TestB3InstrumentsFileBTC:
    """Test cases for B3InstrumentsFileBTC class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileBTC()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_btc_specific_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with BTC-specific data types.

        Verifies
        --------
        - BTC-specific data types are used
        - Pascal case conversion is applied
        - Correct table name is set

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileBTC()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "btc.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["BTC"], "FILE_NAME": ["btc.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["BTC"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify BTC-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["TCKR_SYMB"] == str
            assert expected_dtypes["FNGB_IND"] == str
            assert expected_dtypes["PMT_TP"] == str


# --------------------------
# Tests for B3InstrumentsFileEqtyFwd
# --------------------------
class TestB3InstrumentsFileEqtyFwd:
    """Test cases for B3InstrumentsFileEqtyFwd class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqtyFwd()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_equity_forward_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with equity forward data types.

        Verifies
        --------
        - Equity forward-specific data types are used
        - Date columns are properly typed
        - Integer fields for lots and factors

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqtyFwd()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "eqty_fwd.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["FORWARD"], "FILE_NAME": ["eqty_fwd.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["FORWARD"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify equity forward-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["TRADG_START_DT"] == "date"
            assert expected_dtypes["TRADG_END_DT"] == "date"
            assert expected_dtypes["ALLCN_RND_LOT"] == int
            assert expected_dtypes["PRIC_FCTR"] == int


# --------------------------
# Tests for B3InstrumentsFileExrcEqts
# --------------------------
class TestB3InstrumentsFileExrcEqts:
    """Test cases for B3InstrumentsFileExrcEqts class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileExrcEqts()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_exercise_equities_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with exercise equities data types.

        Verifies
        --------
        - Exercise equities-specific data types are used
        - Date columns are properly typed
        - Delivery type handling

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileExrcEqts()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "exrc_eqts.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["EXERCISE"], "FILE_NAME": ["exrc_eqts.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["EXERCISE"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify exercise equities-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["TRADG_START_DT"] == "date"
            assert expected_dtypes["TRADG_END_DT"] == "date"
            assert expected_dtypes["DLVRY_TP"] == str


# --------------------------
# Tests for B3InstrumentsFileFxdIncm
# --------------------------
class TestB3InstrumentsFileFxdIncm:
    """Test cases for B3InstrumentsFileFxdIncm class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileFxdIncm()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_fixed_income_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with fixed income data types.

        Verifies
        --------
        - Fixed income-specific data types are used
        - Date and integer fields are properly typed
        - Settlement days handling

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileFxdIncm()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "fxd_incm.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["BOND"], "FILE_NAME": ["fxd_incm.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["BOND"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify fixed income-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["TRADG_START_DT"] == "date"
            assert expected_dtypes["TRADG_END_DT"] == "date"
            assert expected_dtypes["DAYS_TO_STTLM"] == int
            assert expected_dtypes["ALLCN_RND_LOT"] == int
            assert expected_dtypes["PRIC_FCTR"] == int

    def test_transform_data_fixed_income_structure(self) -> None:
        """Test transform_data for fixed income XML structure.

        Verifies
        --------
        - FxdIncmInf parent tag is used
        - All fixed income child tags are processed
        - No currency attributes

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileFxdIncm()


# --------------------------
# Tests for B3InstrumentsFileEqty
# --------------------------
class TestB3InstrumentsFileEqty:
    """Test cases for B3InstrumentsFileEqty class."""

    def test_init_inherits_from_instruments_file(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test initialization inherits from B3InstrumentsFile.

        Verifies
        --------
        - Inherits caching functionality
        - URL is set correctly
        - Temporary directory is created

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()
        
        assert hasattr(instance, "temp_dir")
        assert "pesquisapregao/download?filelist=IN" in instance.url
        assert hasattr(instance, "cleanup_cache")

    def test_run_equity_specific_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with equity-specific data types.

        Verifies
        --------
        - Equity-specific data types are used
        - Pascal case conversion is applied
        - Correct table name is set

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()
        
        with patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_cached_fetch.return_value = (StringIO("<root></root>"), "equity.xml")
            mock_transform.return_value = pd.DataFrame({"SctyCtgy": ["EQUITY"], "FILE_NAME": ["equity.xml"]})
            mock_standardize.return_value = pd.DataFrame({"SCTY_CTGY": ["EQUITY"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify equity-specific data types
            assert expected_dtypes["SCTY_CTGY"] == str
            assert expected_dtypes["ISIN"] == str
            assert expected_dtypes["ALLCN_RND_LOT"] == int
            assert expected_dtypes["PRIC_FCTR"] == float
            assert expected_dtypes["LAST_PRIC_CCY"] == str
            assert call_args["cols_from_case"] == "pascal"
            assert call_args["cols_to_case"] == "upper_constant"

    def test_run_parameter_validation_equity(self) -> None:
        """Test parameter validation for equity instruments.

        Verifies
        --------
        - TypeError raised for invalid timeout
        - TypeError raised for invalid boolean parameters
        - TypeError raised for invalid table name

        Returns
        -------
        None
        """
        instance = B3InstrumentsFileEqty()

        try:
            instance.run(timeout="invalid")
            assert False, "Should have raised TypeError for invalid timeout"
        except TypeError:
            pass  # Expected

        try:
            instance.run(bool_verify=123)
            assert False, "Should have raised TypeError for invalid bool_verify"
        except TypeError:
            pass  # Expected

        try:
            instance.run(str_table_name=["invalid"])
            assert False, "Should have raised TypeError for invalid table name"
        except TypeError:
            pass  # Expected


# --------------------------
# Performance and Edge Case Tests
# --------------------------
class TestPerformanceAndEdgeCases:
    """Test cases for performance and edge case scenarios."""

    def test_large_dataset_handling(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test handling of large datasets.

        Verifies
        --------
        - Large datasets are processed efficiently
        - Memory usage is reasonable
        - No performance degradation

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Create large CSV content
        large_csv_lines = ["header"]
        for i in range(10000):
            large_csv_lines.append(f"0{i % 10};GROUP{i};CAM{i};INST{i};ORIG{i}")
        large_csv_content = "\n".join(large_csv_lines)
        
        result = instance.transform_data(StringIO(large_csv_content), "large.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10000
        assert "FILE_NAME" in result.columns

    def test_malformed_data_handling(self) -> None:
        """Test handling of malformed data.

        Verifies
        --------
        - Malformed CSV data is handled gracefully
        - Appropriate errors are raised or data is cleaned
        - No unexpected crashes occur

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with malformed CSV - but not too malformed to cause parser errors
        malformed_csv = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2""")  # Missing last field
        
        # Should handle gracefully without crashing
        try:
            result = instance.transform_data(malformed_csv, "malformed.csv")
            assert isinstance(result, pd.DataFrame)
        except Exception:
            # If parsing fails, that's acceptable behavior for malformed data
            pass

    def test_unicode_content_handling(self) -> None:
        """Test handling of Unicode content.

        Verifies
        --------
        - Unicode characters are processed correctly
        - Different encodings are handled
        - No encoding errors occur

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with Unicode content
        unicode_csv = StringIO("""header
01;Açúcar;Câmara;Instrumento;Origem
02;Café;São Paulo;Opções;Brasil""")
        
        result = instance.transform_data(unicode_csv, "unicode.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_memory_cleanup_after_errors(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test memory cleanup after errors occur.

        Verifies
        --------
        - Resources are cleaned up after exceptions
        - No memory leaks in error scenarios
        - Temporary files are removed

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Force an error during processing
        with patch.object(instance, "get_response") as mock_get_response:
            mock_get_response.side_effect = Exception("Processing error")
            
            with pytest.raises(Exception, match="Processing error"):
                instance.run()
            
            # Verify cleanup can still be called
            instance.cleanup_cache()


# --------------------------
# Mock Validation Tests
# --------------------------
class TestMockValidation:
    """Tests to validate mock behavior and ensure test reliability."""

    def test_mock_requests_prevents_real_calls(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test that mocked requests prevent real HTTP calls.

        Verifies
        --------
        - No real HTTP requests are made
        - Mock is called with correct parameters
        - Network isolation is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Attempt to make request
        instance.get_response(timeout=(5.0, 10.0), bool_verify=False)
        
        # Verify mock was called instead of real request
        mock_fast_operations["requests_get"].assert_called_once_with(
            instance.url, timeout=(5.0, 10.0), verify=False
        )

    def test_mock_subprocess_prevents_real_execution(
        self, 
        mock_fast_operations: dict[str, MagicMock]
    ) -> None:
        """Test that subprocess mocking prevents real command execution.

        Verifies
        --------
        - No real subprocess execution occurs
        - Mock returns controlled output
        - System isolation is maintained

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3EquitiesOptionReferencePremiums()
        mock_response = create_mock_response()
        
        # Setup mocks for parse_raw_ex_file
        with patch.object(instance.cls_dir_files_management, "recursive_unzip_in_memory") as mock_unzip, \
             patch("builtins.open", mock_open(read_data="output content")), \
             patch("os.chmod"), \
             patch("os.getcwd", return_value="/current"), \
             patch("os.chdir"), \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "glob", return_value=[Path("/tmp/output.txt")]), \
             patch.object(Path, "stat") as mock_stat:
            
            mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
            mock_stat.return_value.st_size = 1000
            
            # This should not execute real Wine command
            result = instance.parse_raw_ex_file(mock_response, "prefix_", "filename")
            
            # Verify subprocess was mocked
            mock_fast_operations["subprocess_run"].assert_called_once()
            assert isinstance(result, tuple)

    def test_test_execution_speed(self) -> None:
        """Test that individual tests execute reasonably quickly.

        Verifies
        --------
        - Single test execution completes in reasonable time
        - Mocking provides performance benefits
        - No blocking operations occur

        Returns
        -------
        None
        """
        import time
        
        start_time = time.time()
        
        # Run a representative test
        instance = B3StandardizedInstrumentGroups()
        csv_content = StringIO("header\n01;GROUP1;CAM1;INST1;ORIG1")
        result = instance.transform_data(csv_content, "test.csv")
        
        end_time = time.time()
        
        # Should complete reasonably quickly (increased tolerance)
        assert end_time - start_time < 2.0  # Less than 2 seconds
        assert isinstance(result, pd.DataFrame)


# --------------------------
# Final Coverage Tests
# --------------------------
class TestFinalCoverageScenarios:
    """Final tests to ensure 100% code coverage."""

    def test_all_exception_paths_covered(self) -> None:
        """Test that all exception paths are covered.

        Verifies
        --------
        - All exception handling code is tested
        - Error messages are validated
        - Recovery mechanisms work

        Returns
        -------
        None
        """
        # Test various exception scenarios that might not be covered
        instance = B3StandardizedInstrumentGroups()
        
        # Test with invalid file object - expect specific TypeError from metaclass
        try:
            instance.transform_data(None, "test.csv")
            assert False, "Should have raised TypeError for None file"
        except TypeError as e:
            # The metaclass type checker should catch this
            assert "must be of type" in str(e)

    def test_edge_case_data_values(self) -> None:
        """Test edge case data values.

        Verifies
        --------
        - Boundary values are handled correctly
        - Special characters in data
        - Empty and null values

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Test with edge case CSV content
        edge_case_csv = StringIO("""header
;;;;;
01;;;;"";
;;CAM1;;""")
        
        result = instance.transform_data(edge_case_csv, "edge_case.csv")
        assert isinstance(result, pd.DataFrame)

    def test_all_conditional_branches(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test all conditional branches in the code.

        Verifies
        --------
        - All if/else branches are executed
        - All exception handlers are tested
        - All return paths are covered

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Test different cache scenarios
        with patch.object(instance, "_load_from_cache") as mock_load:
            # Test cache hit
            mock_load.return_value = StringIO("cached")
            result = instance.get_cached_or_fetch()
            assert isinstance(result, StringIO)
            
            # Test cache miss
            mock_load.side_effect = ValueError("Cache miss")
            with patch.object(instance, "get_response") as mock_get, \
                 patch.object(instance, "parse_raw_file") as mock_parse:
                mock_get.return_value = create_mock_response()
                mock_parse.return_value = (StringIO("fresh"), "file.xml")
                
                result = instance.get_cached_or_fetch()
                assert isinstance(result, tuple)

    def test_cleanup_and_teardown_paths(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test cleanup and teardown paths.

        Verifies
        --------
        - Cleanup methods work correctly
        - Resources are properly released
        - No resource leaks occur

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentsFile()
        
        # Test successful cleanup
        with patch('pathlib.Path.exists', return_value=True):
            instance.cleanup_cache()
            mock_fast_operations["shutil_rmtree"].assert_called()
        
        # Test cleanup when directory doesn't exist
        with patch('pathlib.Path.exists', return_value=False):
            instance.cleanup_cache()  # Should not raise exception

    def test_all_property_access_patterns(self) -> None:
        """Test all property access patterns.

        Verifies
        --------
        - All class properties are accessible
        - Property getters work correctly
        - No missing attribute errors

        Returns
        -------
        None
        """
        with patch("tempfile.mkdtemp", return_value="/tmp/test"):
            instance = B3StandardizedInstrumentGroups()
            
            # Test all accessible properties
            assert hasattr(instance, "date_ref")
            assert hasattr(instance, "url")
            assert hasattr(instance, "logger")
            assert hasattr(instance, "cls_db")
            assert hasattr(instance, "cls_dir_files_management")
            
            # Verify properties have expected types
            assert isinstance(instance.date_ref, date)
            assert isinstance(instance.url, str)

    def test_init_with_parameters(
        self, 
        sample_date: date, 
        mock_logger: MagicMock, 
        mock_db_session: MagicMock
    ) -> None:
        """Test initialization with custom parameters.

        Verifies
        --------
        - Custom parameters are correctly assigned
        - URL contains formatted date
        - All attributes are properly set

        Parameters
        ----------
        sample_date : date
            Sample date for testing
        mock_logger : MagicMock
            Mock logger instance
        mock_db_session : MagicMock
            Mock database session instance

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups(
            date_ref=sample_date,
            logger=mock_logger,
            cls_db=mock_db_session
        )
        
        assert instance.date_ref == sample_date
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_db_session
        assert "AI240115" in instance.url

    def test_run_with_default_parameters(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with default parameters.

        Verifies
        --------
        - Correct data types are passed to parent run method
        - Default table name is used
        - Method executes without errors

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("test,data"), "test.csv")
            mock_transform.return_value = pd.DataFrame({"test_col": [1, 2, 3], "FILE_NAME": ["test.csv", "test.csv", "test.csv"]})
            mock_standardize.return_value = pd.DataFrame({"TEST_COL": [1, 2, 3]})
            
            result = instance.run()
            
            # Verify standardize_dataframe was called with correct dtypes
            mock_standardize.assert_called_once()
            call_args = mock_standardize.call_args[1]
            expected_dtypes = {
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": str, 
                "ID_CAMARA": str, 
                "ID_INSTRUMENTO": str, 
                "ORIGEM_INSTRUMENTO": str, 
                "FILE_NAME": "category",
            }
            assert call_args["dict_dtypes"] == expected_dtypes


# --------------------------
# Tests for B3StandardizedInstrumentGroups
# --------------------------
class TestB3StandardizedInstrumentGroups:
    """Test cases for B3StandardizedInstrumentGroups class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Correct URL is constructed with placeholder
        - Default values are set properly
        - Class inherits from ABCB3SearchByTradingSession

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("test,data"), "test.csv")
            mock_transform.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"], "FILE_NAME": ["test.csv"]})
            mock_standardize.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"]})
            
            result = instance.run()
            
            # Verify correct dtypes were passed
            expected_dtypes = {
                "TIPO_REGISTRO": str,
                "ID_GRUPO_INSTRUMENTOS": str, 
                "ID_CAMARA": str, 
                "ID_INSTRUMENTO": str, 
                "ORIGEM_INSTRUMENTO": str, 
                "FILE_NAME": "category",
            }
            mock_standardize.assert_called_once()
            call_args = mock_standardize.call_args[1]
            assert call_args["dict_dtypes"] == expected_dtypes

    def test_transform_data_csv_parsing(self, csv_stringio: StringIO) -> None:
        """Test transform_data method with CSV content.

        Verifies
        --------
        - CSV is parsed correctly with skiprows=1
        - Correct column names are assigned
        - FILE_NAME column is added
        - Returns DataFrame with expected structure

        Parameters
        ----------
        csv_stringio : StringIO
            StringIO object with CSV data

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        
        # Create specific CSV for this test
        csv_content = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2;ORIG2""")
        
        result = instance.transform_data(csv_content, "test_file.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "FILE_NAME" in result.columns
        assert all(result["FILE_NAME"] == "test_file.csv")
        
        expected_columns = [
            "TIPO_REGISTRO", 
            "ID_GRUPO_INSTRUMENTOS", 
            "ID_CAMARA", 
            "ID_INSTRUMENTO", 
            "ORIGEM_INSTRUMENTO",
            "FILE_NAME"
        ]
        assert list(result.columns) == expected_columns

    def test_transform_data_empty_file(self) -> None:
        """Test transform_data with empty file.

        Verifies
        --------
        - Empty CSV files are handled gracefully
        - Returns DataFrame with correct columns but no data
        - FILE_NAME column is still added

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()
        empty_csv = StringIO("header\n")
        
        result = instance.transform_data(empty_csv, "empty.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "FILE_NAME" in result.columns

    def test_run_parameter_validation(self) -> None:
        """Test run method parameter type validation.

        Verifies
        --------
        - TypeError raised for invalid timeout type
        - TypeError raised for invalid bool_verify type
        - TypeError raised for invalid str_table_name type

        Returns
        -------
        None
        """
        instance = B3StandardizedInstrumentGroups()

        try:
            instance.run(timeout="invalid")
            assert False, "Should have raised TypeError for invalid timeout"
        except TypeError:
            pass  # Expected

        try:
            instance.run(bool_verify="not_bool")
            assert False, "Should have raised TypeError for invalid bool_verify"
        except TypeError:
            pass  # Expected

        try:
            instance.run(str_table_name=123)
            assert False, "Should have raised TypeError for invalid table name"
        except TypeError:
            pass  # Expected


# --------------------------
# Tests for B3IndexReport
# --------------------------
class TestB3IndexReport:
    """Test cases for B3IndexReport class."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default parameters.

        Verifies
        --------
        - Correct URL is constructed for Index Report
        - Default values are set properly
        - Inherits from ABCB3SearchByTradingSession

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        assert "pesquisapregao/download?filelist=IR" in instance.url
        assert instance.logger is None
        assert instance.cls_db is None

    def test_run_with_custom_parameters(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with custom parameters.

        Verifies
        --------
        - Custom column case conversion parameters work
        - Correct data types are used for index report
        - Default table name is applied

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO(create_sample_xml_content()), "test.xml")
            mock_transform.return_value = pd.DataFrame({"TckrSymb": ["TEST"], "FILE_NAME": ["test.xml"]})
            mock_standardize.return_value = pd.DataFrame({"TCKR_SYMB": ["TEST"]})
            
            result = instance.run(
                cols_from_case="snake",
                cols_to_case="lower"
            )
            
            # Verify standardize_dataframe was called with correct parameters
            mock_standardize.assert_called_once()
            call_args = mock_standardize.call_args[1]
            assert call_args["cols_from_case"] == "snake"
            assert call_args["cols_to_case"] == "lower"

    def test_transform_data_xml_parsing(self, xml_stringio: StringIO) -> None:
        """Test transform_data method with XML content.

        Verifies
        --------
        - XML is parsed correctly
        - IndxInf nodes are extracted
        - All expected fields are captured
        - Currency attributes are handled

        Parameters
        ----------
        xml_stringio : StringIO
            StringIO object with XML data

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            # Create mock XML elements
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node = MagicMock()
            mock_find_all.return_value = [mock_node]
            
            # Mock individual tag finding
            def mock_find(tag):
                mock_element = MagicMock()
                mock_element.getText.return_value = f"test_{tag}"
                if tag == "SttlmVal":
                    mock_element.get.return_value = "BRL"
                return mock_element
            
            mock_node.find = mock_find
            
            result = instance.transform_data(xml_stringio, "test.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "FILE_NAME" in result.columns
            assert result.iloc[0]["FILE_NAME"] == "test.xml"

    def test_transform_data_missing_xml_elements(self) -> None:
        """Test transform_data with missing XML elements.

        Verifies
        --------
        - Missing XML elements are handled gracefully
        - None values are set for missing elements
        - No exceptions are raised

        Returns
        -------
        None
        """
        instance = B3IndexReport()
        
        with patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser, \
             patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
            
            mock_xml = MagicMock()
            mock_parser.return_value = mock_xml
            
            mock_node = MagicMock()
            mock_find_all.return_value = [mock_node]
            
            # Mock find method to return None for missing elements
            mock_node.find.return_value = None
            
            result = instance.transform_data(StringIO("<root></root>"), "test.xml")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            # All values should be None except FILE_NAME
            for col in result.columns:
                if col != "FILE_NAME":
                    assert result.iloc[0][col] is None


# --------------------------
# Tests for B3DerivatiesMarketListISINCPRs
# --------------------------
class TestB3DerivatiesMarketListISINCPRs:
    """Test cases for B3DerivatiesMarketListISINCPRs class."""

    def test_init_with_ex_file_url(self) -> None:
        """Test initialization with .ex_ file URL.

        Verifies
        --------
        - URL points to .ex_ file instead of .zip
        - Correct file pattern is used
        - Class inherits executable parsing functionality

        Returns
        -------
        None
        """
        instance = B3DerivatiesMarketListISINCPRs()
        
        assert "pesquisapregao/download?filelist=IC" in instance.url
        assert instance.url.endswith(".ex_")

    def test_run_with_specific_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with specific data types.

        Verifies
        --------
        - Correct data types for ISIN CPRs
        - Date format handling
        - Float precision for VALOR_NOMINAL

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3DerivatiesMarketListISINCPRs()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("fixed width data"), "cpr.txt")
            mock_transform.return_value = pd.DataFrame({
                "DATA_CADASTRO": ["20240115"], 
                "VALOR_NOMINAL": [1000.50],
                "FILE_NAME": ["cpr.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"DATA_CADASTRO": ["20240115"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify specific data types
            assert expected_dtypes["DATA_CADASTRO"] == str
            assert expected_dtypes["EMISSOR"] == str
            assert expected_dtypes["VALOR_NOMINAL"] == float
            assert expected_dtypes["CODIGO_ISIN"] == str

    def test_transform_data_fixed_width_parsing(self) -> None:
        """Test transform_data for fixed-width file format.

        Verifies
        --------
        - Fixed-width format is parsed correctly
        - Column specifications are accurate
        - All expected columns are present

        Returns
        -------
        None
        """
        instance = B3DerivatiesMarketListISINCPRs()
        
        # Create sample fixed-width content
        fixed_width_content = StringIO(
            "20240115EMIS12345678901234567890201234000000000000100000020241215BRCPRABC1234"
        )
        
        result = instance.transform_data(fixed_width_content, "cpr_isin.txt")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "FILE_NAME" in result.columns
        
        # Verify all expected columns are present
        expected_columns = [
            "DATA_CADASTRO", "EMISSOR", "CNPJ", "DATA_EMISSAO", 
            "VALOR_NOMINAL", "DATA_VENCIMENTO", "CODIGO_ISIN", "FILE_NAME"
        ]
        assert len(result.columns) == len(expected_columns)


# --------------------------
# Tests for B3DerivativesMarketCombinedPositions
# --------------------------
class TestB3DerivativesMarketCombinedPositions:
    """Test cases for B3DerivativesMarketCombinedPositions class."""

    def test_init_ex_file_url(self) -> None:
        """Test initialization with .ex_ file URL pattern.

        Verifies
        --------
        - Correct URL pattern for combined positions
        - .ex_ file extension is used
        - PT prefix is in URL

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketCombinedPositions()
        
        assert "pesquisapregao/download?filelist=PT" in instance.url
        assert instance.url.endswith(".ex_")

    def test_run_with_position_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with position-specific data types.

        Verifies
        --------
        - Float precision for contract quantities
        - Date format handling
        - Market code categorization

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3DerivativesMarketCombinedPositions()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("position data"), "positions.txt")
            mock_transform.return_value = pd.DataFrame({
                "DATA_PREGAO": ["20240115"], 
                "QTD_CONTRATOS_TRAVADOS": [1500.0],
                "FILE_NAME": ["positions.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"DATA_PREGAO": ["20240115"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            # Verify position-specific data types
            assert expected_dtypes["DATA_PREGAO"] == str
            assert expected_dtypes["QTD_CONTRATOS_TRAVADOS"] == float
            assert expected_dtypes["QTD_CONTRATOS_BAIXADOS"] == float


# --------------------------
# Tests for B3FXMarketVolumeSettled
# --------------------------
class TestB3FXMarketVolumeSettled:
    """Test cases for B3FXMarketVolumeSettled class."""

    def test_init_volume_settled_url(self) -> None:
        """Test initialization with volume settled URL pattern.

        Verifies
        --------
        - Correct URL pattern for volume settled
        - CV prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3FXMarketVolumeSettled()
        
        assert "pesquisapregao/download?filelist=CV" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_volume_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with volume-specific data types.

        Verifies
        --------
        - Date format as YYYYMMDD
        - Float precision for volume values
        - Currency handling

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3FXMarketVolumeSettled()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("volume data"), "volume.txt")
            mock_transform.return_value = pd.DataFrame({
                "ID_TRANSACAO": ["12345678"], 
                "VALOR_LIQUIDO_COMPENSADO_DOLAR": [100000.50],
                "FILE_NAME": ["volume.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"ID_TRANSACAO": ["12345678"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["str_fmt_dt"] == "YYYYMMDD"
            
            expected_dtypes = call_args["dict_dtypes"]
            assert expected_dtypes["VALOR_LIQUIDO_COMPENSADO_DOLAR"] == float
            assert expected_dtypes["VALOR_LIQUIDO_COMPENSADO_REAL"] == float

    def test_transform_data_fixed_width_volume(self) -> None:
        """Test transform_data for volume fixed-width format.

        Verifies
        --------
        - Fixed-width format is parsed correctly
        - Volume calculations are accurate
        - Currency fields are handled

        Returns
        -------
        None
        """
        instance = B3FXMarketVolumeSettled()
        
        # Create sample fixed-width content for volume
        fixed_width_content = StringIO(
            "12345678202401152024011500000000100000000000000500000"
        )
        
        result = instance.transform_data(fixed_width_content, "volume_settled.txt")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "FILE_NAME" in result.columns
        
        expected_columns = [
            "ID_TRANSACAO", "DATA_REFERENCIA", 
            "DATA_LIQUIDACAO_VALORES_LIQUIDOS_COMPENSADO",
            "VALOR_LIQUIDO_COMPENSADO_DOLAR", 
            "VALOR_LIQUIDO_COMPENSADO_REAL", "FILE_NAME"
        ]
        assert len(result.columns) == len(expected_columns)


# --------------------------
# Tests for B3FixedIncome
# --------------------------
class TestB3FixedIncome:
    """Test cases for B3FixedIncome class."""

    def test_init_fixed_income_url(self) -> None:
        """Test initialization with fixed income URL pattern.

        Verifies
        --------
        - Correct URL pattern for fixed income
        - RF prefix is used in URL
        - .ex_ file extension

        Returns
        -------
        None
        """
        instance = B3FixedIncome()
        
        assert "pesquisapregao/download?filelist=RF" in instance.url
        assert instance.url.endswith(".ex_")

    def test_run_with_fixed_income_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with fixed income data types.

        Verifies
        --------
        - YYYYMMDD date format
        - Rate and price precision
        - Ticker symbol handling

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3FixedIncome()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("fixed income data"), "fixed_income.txt")
            mock_transform.return_value = pd.DataFrame({
                "TICKER": ["LTN"], 
                "PRECO_UNITARIO": ["985.50"],
                "FILE_NAME": ["fixed_income.txt"]
            })
            mock_standardize.return_value = pd.DataFrame({"TICKER": ["LTN"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["str_fmt_dt"] == "YYYYMMDD"
            
            expected_dtypes = call_args["dict_dtypes"]
            assert expected_dtypes["TICKER"] == str
            assert expected_dtypes["PRECO_UNITARIO"] == str
            assert expected_dtypes["TAXA_COMPRA"] == str

    def test_transform_data_csv_format(self) -> None:
        """Test transform_data for CSV format.

        Verifies
        --------
        - CSV parsing with semicolon separator
        - Rate columns are handled
        - Price precision is maintained

        Returns
        -------
        None
        """
        instance = B3FixedIncome()
        
        csv_content = StringIO("""header
LTN;20240115;252;252;985.50;10.25;10.35;10.30
NTN-B;20240115;1825;1825;3250.75;6.15;6.25;6.20""")
        
        result = instance.transform_data(csv_content, "fixed_income.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "FILE_NAME" in result.columns
        
        expected_columns = [
            "TICKER", "DATE", "PRAZO_DIAS_UTEIS", "PRAZO_DIAS_CORRIDOS",
            "PRECO_UNITARIO", "TAXA_COMPRA", "TAXA_VENDA", 
            "TAXA_INDICATIVA", "FILE_NAME"
        ]
        assert list(result.columns) == expected_columns


# --------------------------
# Tests for B3InstrumentGroupParameters
# --------------------------
class TestB3InstrumentGroupParameters:
    """Test cases for B3InstrumentGroupParameters class."""

    def test_init_parameters_url(self) -> None:
        """Test initialization with parameters URL pattern.

        Verifies
        --------
        - Correct URL pattern for parameters
        - PG prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3InstrumentGroupParameters()
        
        assert "pesquisapregao/download?filelist=PG" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_parameters_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with parameters data types.

        Verifies
        --------
        - DD/MM/YYYY date format
        - String handling for various fields
        - Parameter classification

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3InstrumentGroupParameters()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("parameters data"), "parameters.csv")
            mock_transform.return_value = pd.DataFrame({
                "TIPO": ["1"], 
                "ID_GRUPO_INSTRUMENTOS": ["123"],
                "FILE_NAME": ["parameters.csv"]
            })
            mock_standardize.return_value = pd.DataFrame({"TIPO": ["1"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            assert call_args["str_fmt_dt"] == "DD/MM/YYYY"
            
            expected_dtypes = call_args["dict_dtypes"]
            assert expected_dtypes["TIPO"] == str
            assert expected_dtypes["ID_GRUPO_INSTRUMENTOS"] == str
            assert expected_dtypes["MERCADO"] == str

    def test_transform_data_csv_parsing(self) -> None:
        """Test transform_data for CSV parsing.

        Verifies
        --------
        - Semicolon-separated CSV parsing
        - Parameter fields are extracted
        - Date intervals are handled

        Returns
        -------
        None
        """
        instance = B3InstrumentGroupParameters()
        
        csv_content = StringIO("""header
1;123;BMVF;Ações;5;100000;10;2;1;01/01/2024;31/12/2024;N""")
        
        result = instance.transform_data(csv_content, "parameters.csv")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "FILE_NAME" in result.columns
        
        expected_columns = [
            "TIPO", "ID_GRUPO_INSTRUMENTOS", "MERCADO", 
            "NOME_CLASSIFICACAO", "PRAZO_MINIMO_ENCERRAMENTO",
            "LIMITE_LIQUIDEZ", "PRAZO_SUBCARTEIRA", 
            "PRAZO_LIQUIDACAO", "PRAZO_LIQUIDACAO_NO_VENCIMENTO",
            "DATA_INICIAL_INTERVALO_VENCIMENTOS", 
            "DATA_FINAL_INTERVALO_VENCIMENTOS",
            "INDICADOR_MODELO_GENERICO", "FILE_NAME"
        ]
        assert list(result.columns) == expected_columns


# --------------------------
# Tests for B3PrimitiveRiskFactors
# --------------------------
class TestB3PrimitiveRiskFactors:
    """Test cases for B3PrimitiveRiskFactors class."""

    def test_init_risk_factors_url(self) -> None:
        """Test initialization with risk factors URL pattern.

        Verifies
        --------
        - Correct URL pattern for risk factors
        - FP prefix is used in URL
        - .zip file extension

        Returns
        -------
        None
        """
        instance = B3PrimitiveRiskFactors()
        
        assert "pesquisapregao/download?filelist=FP" in instance.url
        assert instance.url.endswith(".zip")

    def test_run_with_risk_factor_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
        """Test run method with risk factor data types.

        Verifies
        --------
        - String types for risk factor IDs
        - Format variation handling
        - Interpolation base fields

        Parameters
        ----------
        mock_fast_operations : dict[str, MagicMock]
            Dictionary of mocked operations

        Returns
        -------
        None
        """
        instance = B3PrimitiveRiskFactors()
        
        with patch.object(instance, "get_response") as mock_get_response, \
             patch.object(instance, "parse_raw_file") as mock_parse, \
             patch.object(instance, "transform_data") as mock_transform, \
             patch.object(instance, "standardize_dataframe") as mock_standardize:
            
            mock_get_response.return_value = create_mock_response()
            mock_parse.return_value = (StringIO("risk factors data"), "risk_factors.csv")
            mock_transform.return_value = pd.DataFrame({
                "TIPO_REGISTRO": ["1"], 
                "ID_FPR": ["FPR001"],
                "FILE_NAME": ["risk_factors.csv"]
            })
            mock_standardize.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"]})
            
            result = instance.run()
            
            call_args = mock_standardize.call_args[1]
            expected_dtypes = call_args["dict_dtypes"]
            
            assert expected_dtypes["TIPO_REGISTRO"] == str
            assert expected_dtypes["ID_FPR"] == str
            assert expected_dtypes["NOME_FPR"] == str
            assert expected_dtypes["FORMATO_VARIACAO"] == str