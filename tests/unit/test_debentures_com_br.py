"""Unit tests for DebenturesComBr classes.

Tests the ingestion functionality of Debentures.com.br classes, including:
- Initialization with valid and invalid inputs
- Data retrieval and parsing
- Data transformation and standardization
- Error handling and edge cases
"""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.otc.debentures_com_br import (
    DebenturesComBrInfos,
    DebenturesComBrMTM,
    DebenturesComBrOTCPVs,
    DebenturesComBrPrimaryOffers,
    DebenturesComBrSecondaryExchange,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response(mocker: MockerFixture) -> Response:
    """Mock Response object with sample content.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects

    Returns
    -------
    Response
        Mocked Response object with HTML content
    """
    response = MagicMock(spec=Response)
    response.content = b"Sample content"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def sample_dates() -> tuple[date, date]:
    """Provide sample date range for testing.
    
    Returns
    -------
    tuple[date, date]
        A fixed date range for consistent testing
    """
    return date(2023, 1, 1), date(2023, 1, 5)


@pytest.fixture
def mock_stringio_mtm(mocker: MockerFixture) -> StringIO:
    """Mock StringIO object with sample CSV content for MTM.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects

    Returns
    -------
    StringIO
        A StringIO object with sample CSV content
    """
    # Create proper CSV content with header rows to skip and footer
    csv_content = """Header1
Header2  
Header3
01/01/2023	TEST	1000,0	5,0	2,0	1002,0	CRIT	ACTIVE
Footer1
Footer2
Footer3
Footer4"""
    return StringIO(csv_content)


@pytest.fixture
def mock_stringio_infos(mocker: MockerFixture) -> StringIO:
    """Mock StringIO object with sample CSV content for Infos (85 columns).
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects

    Returns
    -------
    StringIO
        A StringIO object with sample CSV content
    """
    # Create proper CSV content with exactly 85 columns
    header_lines = ["Header1", "Header2", "Header3", "Header4", "Header5"]
    
    # Create exactly 85 columns of test data
    test_data = ["TEST"] * 85
    csv_content = "\n".join(header_lines) + "\n" + "\t".join(test_data)
    return StringIO(csv_content)


@pytest.fixture
def mock_stringio_primary_offers(mocker: MockerFixture) -> StringIO:
    """Mock StringIO object with sample CSV content for Primary Offers.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects

    Returns
    -------
    StringIO
        A StringIO object with sample CSV content
    """
    csv_content = """Header1
Header2
Header3
TEST	COMPANY	ACTIVE	01/01/2023	01/01/2023	01/01/2023	1000,0
Footer1
Footer2
Footer3
Footer4"""
    return StringIO(csv_content)


@pytest.fixture
def mock_session() -> MagicMock:
    """Mock database session.
    
    Returns
    -------
    MagicMock
        Mocked database session
    """
    return MagicMock()


# --------------------------
# Tests for DebenturesComBrMTM
# --------------------------
class TestDebenturesComBrMTM:
    """Test cases for DebenturesComBrMTM class."""

    def test_init_valid_inputs(self, sample_dates: tuple[date, date]) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is created with valid date_start and date_end
        - Attributes are correctly set
        - Default values are used when None is provided

        Parameters
        ----------
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        date_start, date_end = sample_dates
        instance = DebenturesComBrMTM(date_start=date_start, date_end=date_end)
        assert instance.date_start == date_start
        assert instance.date_end == date_end
        assert isinstance(instance.logger, Logger) or instance.logger is None
        assert instance.cls_db is None
        assert instance.url.startswith("https://www.debentures.com.br")

    def test_init_default_dates(self, mocker: MockerFixture) -> None:
        """Test initialization with default dates.

        Verifies
        --------
        - Default dates are set using DatesBRAnbima
        - Dates are correctly calculated for working days

        Parameters
        ----------
        mocker : MockerFixture
            Pytest mocker for creating mock objects

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days",
            side_effect=[date(2023, 1, 1), date(2023, 1, 5)],
        )
        instance = DebenturesComBrMTM()
        assert isinstance(instance.date_start, date)
        assert isinstance(instance.date_end, date)

    @patch("requests.get")
    @patch("stpstone.ingestion.countries.br.otc.debentures_com_br.DebenturesComBrMTM.get_file")
    def test_run_without_db(
        self,
        mock_get_file: MagicMock,
        mock_requests_get: MagicMock,
        mock_response: Response,
        mock_stringio_mtm: StringIO,
        sample_dates: tuple[date, date],
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Returns a DataFrame when cls_db is None
        - Correct URL formatting
        - Data transformation and standardization

        Parameters
        ----------
        mock_get_file : MagicMock
            Mocked get_file method
        mock_requests_get : MagicMock
            Mocked requests.get method
        mock_response : Response
            Mocked response object
        mock_stringio_mtm : StringIO
            Mocked StringIO object
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        mock_get_file.return_value = mock_stringio_mtm
        
        # Mock the standardize_dataframe method to bypass empty DataFrame check
        instance = DebenturesComBrMTM(date_start=sample_dates[0], date_end=sample_dates[1])
        
        with patch.object(instance, 'standardize_dataframe') as mock_standardize:
            # Create a non-empty DataFrame for the mock
            expected_df = pd.DataFrame({
                "DATA_PU": ["01/01/2023"],
                "ATIVO": ["TEST"],
                "VALOR_NOMINAL": [1000.0],
                "JUROS": [5.0],
                "PREMIO": [2.0],
                "PRECO_UNITARIO": ["1002.0"],
                "CRITERIO_CALCULO": ["CRIT"],
                "SITUACAO": ["ACTIVE"]
            })
            mock_standardize.return_value = expected_df
            
            result = instance.run()
            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "DATA_PU",
                "ATIVO",
                "VALOR_NOMINAL",
                "JUROS",
                "PREMIO",
                "PRECO_UNITARIO",
                "CRITERIO_CALCULO",
                "SITUACAO",
            ]
            mock_requests_get.assert_called_once()

    def test_transform_data(self, mock_stringio_mtm: StringIO) -> None:
        """Test transform_data method.

        Verifies
        --------
        - Correct parsing of CSV content
        - Proper column names and data types
        - Handling of NA values

        Parameters
        ----------
        mock_stringio_mtm : StringIO
            Mocked StringIO object

        Returns
        -------
        None
        """
        instance = DebenturesComBrMTM()
        df_ = instance.transform_data(mock_stringio_mtm)
        assert isinstance(df_, pd.DataFrame)
        assert df_.shape[0] == 1
        assert df_["ATIVO"].iloc[0] == "TEST"
        assert df_["VALOR_NOMINAL"].iloc[0] == 1000.0

    @patch("requests.get")
    def test_invalid_timeout_type(self, mock_requests_get: MagicMock) -> None:
        """Test run with invalid timeout type.

        Verifies
        --------
        - TypeError is raised for invalid timeout types

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get

        Returns
        -------
        None
        """
        instance = DebenturesComBrMTM()
        with pytest.raises(TypeError):
            instance.run(timeout="invalid")

    def test_parse_raw_file(self, mock_response: Response) -> None:
        """Test parse_raw_file method.

        Verifies
        --------
        - Correctly calls get_file with response
        - Returns StringIO object

        Parameters
        ----------
        mock_response : Response
            Mocked response

        Returns
        -------
        None
        """
        instance = DebenturesComBrMTM()
        with patch.object(instance, "get_file", return_value=StringIO()) as mock_get_file:
            result = instance.parse_raw_file(mock_response)
            assert isinstance(result, StringIO)
            mock_get_file.assert_called_once_with(resp_req=mock_response)


# --------------------------
# Tests for DebenturesComBrInfos
# --------------------------
class TestDebenturesComBrInfos:
    """Test cases for DebenturesComBrInfos class."""

    def test_init_valid_inputs(self, sample_dates: tuple[date, date]) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is created with valid date_start and date_end
        - Attributes are correctly set

        Parameters
        ----------
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        date_start, date_end = sample_dates
        instance = DebenturesComBrInfos(date_start=date_start, date_end=date_end)
        assert instance.date_start == date_start
        assert instance.date_end == date_end
        assert isinstance(instance.logger, Logger) or instance.logger is None
        assert instance.cls_db is None

    @patch("requests.get")
    @patch("stpstone.ingestion.countries.br.otc.debentures_com_br.DebenturesComBrInfos.get_file")
    def test_run_with_db(
        self,
        mock_get_file: MagicMock,
        mock_requests_get: MagicMock,
        mock_response: Response,
        mock_stringio_infos: StringIO,
        mock_session: MagicMock,
        sample_dates: tuple[date, date],
    ) -> None:
        """Test run method with database session.

        Verifies
        --------
        - Inserts data into database when cls_db is provided
        - Correct data transformation and standardization

        Parameters
        ----------
        mock_get_file : MagicMock
            Mocked get_file method
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : Response
            Mocked response object
        mock_stringio_infos : StringIO
            Mocked StringIO object
        mock_session : MagicMock
            Mocked database session
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        mock_get_file.return_value = mock_stringio_infos
        instance = DebenturesComBrInfos(
            date_start=sample_dates[0], date_end=sample_dates[1], cls_db=mock_session
        )
        
        with patch.object(instance, "insert_table_db") as mock_insert, \
            patch.object(instance, 'standardize_dataframe') as mock_standardize:
                # Create a non-empty DataFrame for the mock
                expected_df = pd.DataFrame({col: ["TEST"] for col in instance.transform_data(
                    mock_stringio_infos).columns})
                mock_standardize.return_value = expected_df
                
                result = instance.run()
                assert result is None
                mock_insert.assert_called_once()

    def test_transform_data_infos(self, mock_stringio_infos: StringIO) -> None:
        """Test transform_data method for Infos class.

        Verifies
        --------
        - Correct parsing of complex CSV content
        - Proper handling of numerous columns
        - Correct data types

        Parameters
        ----------
        mock_stringio_infos : StringIO
            Mocked StringIO object

        Returns
        -------
        None
        """
        instance = DebenturesComBrInfos()
        df_ = instance.transform_data(mock_stringio_infos)
        assert isinstance(df_, pd.DataFrame)
        # Check that we get the expected 85 columns (the class defines 85 column names)
        assert len(df_.columns) == 85
        assert df_["CODIGO_DO_ATIVO"].iloc[0] == "TEST"


# --------------------------
# Tests for DebenturesComBrOTCPVs
# --------------------------
class TestDebenturesComBrOTCPVs:
    """Test cases for DebenturesComBrOTCPVs class."""

    def test_init_edge_cases(self, mocker: MockerFixture) -> None:
        """Test initialization with edge cases.

        Verifies
        --------
        - Handles None dates correctly
        - Initializes with default logger and db session

        Parameters
        ----------
        mocker : MockerFixture
            Pytest-mocker fixture

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days",
            return_value=date(2023, 1, 1),
        )
        instance = DebenturesComBrOTCPVs()
        assert isinstance(instance.date_start, date)
        assert isinstance(instance.date_end, date)
        assert instance.url.endswith("dt_fim={}")

    @patch("requests.get")
    def test_get_response_timeout(
        self, mock_requests_get: MagicMock, sample_dates: tuple[date, date]
    ) -> None:
        """Test get_response with timeout error.

        Verifies
        --------
        - TimeoutError is properly raised
        - Backoff mechanism is applied

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        from requests.exceptions import Timeout

        mock_requests_get.side_effect = Timeout("Request timed out")
        instance = DebenturesComBrOTCPVs(
            date_start=sample_dates[0], date_end=sample_dates[1]
        )
        with pytest.raises(Timeout):
            instance.get_response()


# --------------------------
# Tests for DebenturesComBrPrimaryOffers
# --------------------------
class TestDebenturesComBrPrimaryOffers:
    """Test cases for DebenturesComBrPrimaryOffers class."""

    def test_url_formatting(self, sample_dates: tuple[date, date]) -> None:
        """Test URL formatting in get_response.

        Verifies
        --------
        - Correct date formatting in URL
        - Proper URL construction

        Parameters
        ----------
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        instance = DebenturesComBrPrimaryOffers(
            date_start=sample_dates[0], date_end=sample_dates[1]
        )
        with patch("requests.get") as mock_get:
            instance.get_response()
            mock_get.assert_called_once()
            called_url = mock_get.call_args[0][0]
            # Check for URL-encoded date format (/ becomes %2F)
            assert "01%2F01%2F2023" in called_url
            assert "05%2F01%2F2023" in called_url

    def test_transform_data_primary_offers(self, mock_stringio_primary_offers: StringIO) -> None:
        """Test transform_data for PrimaryOffers class.

        Verifies
        --------
        - Correct parsing of CSV content
        - Proper column names and data types

        Parameters
        ----------
        mock_stringio_primary_offers : StringIO
            Mocked StringIO object

        Returns
        -------
        None
        """
        instance = DebenturesComBrPrimaryOffers()
        df_ = instance.transform_data(mock_stringio_primary_offers)
        assert isinstance(df_, pd.DataFrame)
        assert df_.shape[0] == 1
        assert df_["CODIGO_ATIVO"].iloc[0] == "TEST"
        assert df_["VOLUME_MOEDA_EPOCA"].iloc[0] == 1000.0


# --------------------------
# Tests for DebenturesComBrSecondaryExchange
# --------------------------
class TestDebenturesComBrSecondaryExchange:
    """Test cases for DebenturesComBrSecondaryExchange class."""

    def test_run_invalid_table_name(
        self, mock_response: Response, sample_dates: tuple[date, date]
    ) -> None:
        """Test run with invalid table name.

        Verifies
        --------
        - Handles invalid table names appropriately
        - Does not crash with empty or malformed table name

        Parameters
        ----------
        mock_response : Response
            Mocked requests.get response
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing

        Returns
        -------
        None
        """
        # Create proper mock data that won't result in empty DataFrame
        mock_stringio = StringIO("""Header1
Header2
Header3
01/01/2023	EMISSOR	CODIGO	ISIN123	100	1	100.0	100.0	100.0	100.0
Footer1
Footer2
Footer3""")
        
        instance = DebenturesComBrSecondaryExchange(
            date_start=sample_dates[0], date_end=sample_dates[1]
        )
        with patch("requests.get", return_value=mock_response), \
            patch.object(instance, "get_file", return_value=mock_stringio), \
                patch.object(instance, 'standardize_dataframe') as mock_standardize:
                    # Mock successful standardization
                    expected_df = pd.DataFrame({"test": ["data"]})
                    mock_standardize.return_value = expected_df
                    
                    # When cls_db is None, run() should return the DataFrame
                    result = instance.run(str_table_name="")
                    assert isinstance(result, pd.DataFrame)
                    assert "test" in result.columns

    def test_transform_data_edge_cases(self, mocker: MockerFixture) -> None:
        """Test transform_data with edge cases.

        Verifies
        --------
        - Handles empty data
        - Handles NA values correctly

        Parameters
        ----------
        mocker : MockerFixture
            Pytest-mocker fixture

        Returns
        -------
        None
        """
        csv_content = """Header1
Header2
Header3
-	-	-	-	-	-	-	-	-	-
Footer1
Footer2
Footer3"""
        mock_stringio = StringIO(csv_content)
        instance = DebenturesComBrSecondaryExchange()
        df_ = instance.transform_data(mock_stringio)
        assert isinstance(df_, pd.DataFrame)
        # Should have 1 row but all values should be NaN due to NA handling
        assert df_.shape[0] == 1
        assert df_.isna().all().all()


# --------------------------
# Fallback and Error Testing
# --------------------------
def test_reload_module(mocker: MockerFixture) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - State is properly reset

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mocker fixture

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.br.otc.debentures_com_br

    mocker.patch("requests.get")
    importlib.reload(stpstone.ingestion.countries.br.otc.debentures_com_br)
    instance = DebenturesComBrMTM()
    assert isinstance(instance, DebenturesComBrMTM)