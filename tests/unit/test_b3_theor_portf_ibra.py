"""Unit tests for B3TheoricalPortfolioIBRA module.

Tests the ingestion functionality for the B3 IBRA theoretical portfolio, covering:
- Initialization with valid inputs and None date_ref
- run with and without cls_db
- get_response success, HTTP error, and timeout variants
- parse_raw_file valid and invalid (mocked resp_req.json side_effect)
- transform_data normal and empty input
- Module reload test
"""

from datetime import date
import importlib
import sys
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response
from sqlalchemy.orm import Session

from stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibra import B3TheoricalPortfolioIBRA
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement


_EXPECTED_URL = (
    "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
    "eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgi"
    "OiJJQlJBIiwic2VnbWVudCI6IjEifQ=="
)

_SAMPLE_JSON = {
    "results": [
        {"segment": "Stock", "cod": "PETR4", "asset": "Petrobras", "part": 5.2},
        {"segment": "Stock", "cod": "VALE3", "asset": "Vale", "part": 4.8},
    ],
    "header": {"date": "01/01/23", "text": "Test"},
    "page": {"pageNumber": 1, "pageSize": 120, "totalRecords": 2, "totalPages": 1},
}


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> None:
    """Mock backoff to bypass retry delays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mock object for requests.get
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_response() -> MagicMock:
    """Mock Response object with sample B3 portfolio JSON.

    Returns
    -------
    MagicMock
        Mocked requests Response object
    """
    response = MagicMock(spec=Response)
    response.json.return_value = _SAMPLE_JSON
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def sample_date() -> date:
    """Provide a fixed date for consistent testing.

    Returns
    -------
    date
        Sample date object
    """
    return date(2023, 1, 1)


@pytest.fixture
def mock_logger() -> MagicMock:
    """Mock Logger object for testing.

    Returns
    -------
    MagicMock
        Mocked logger object
    """
    return MagicMock()


@pytest.fixture
def mock_db_session() -> MagicMock:
    """Mock database session for testing.

    Returns
    -------
    MagicMock
        Mocked SQLAlchemy session
    """
    mock_session = MagicMock(spec=Session)
    mock_session.insert = MagicMock()
    return mock_session


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provide a sample DataFrame matching the IBRA portfolio structure.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with representative portfolio columns
    """
    return pd.DataFrame(
        {
            "SEGMENT": ["Stock"],
            "COD": ["PETR4"],
            "ASSET": ["Petrobras"],
            "PART": [5.2],
        }
    )


# --------------------------
# Tests
# --------------------------
class TestB3TheoricalPortfolioIBRA:
    """Test cases for B3TheoricalPortfolioIBRA."""

    @pytest.fixture
    def instance_no_db(
        self, sample_date: date, mock_logger: MagicMock
    ) -> B3TheoricalPortfolioIBRA:
        """Fixture providing instance without database.

        Parameters
        ----------
        sample_date : date
            Sample date for initialization
        mock_logger : MagicMock
            Mocked logger

        Returns
        -------
        B3TheoricalPortfolioIBRA
            Initialized instance without database
        """
        return B3TheoricalPortfolioIBRA(date_ref=sample_date, logger=mock_logger, cls_db=None)

    @pytest.fixture
    def instance(
        self,
        sample_date: date,
        mock_logger: MagicMock,
        mock_db_session: MagicMock,
    ) -> B3TheoricalPortfolioIBRA:
        """Fixture providing instance with database.

        Parameters
        ----------
        sample_date : date
            Sample date for initialization
        mock_logger : MagicMock
            Mocked logger
        mock_db_session : MagicMock
            Mocked database session

        Returns
        -------
        B3TheoricalPortfolioIBRA
            Initialized instance with database
        """
        return B3TheoricalPortfolioIBRA(
            date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
        )

    def test_init_with_valid_inputs(
        self, instance: B3TheoricalPortfolioIBRA, sample_date: date
    ) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - date_ref, logger, and cls_db are set correctly
        - url resolves to the IBRA class-level constant
        - Helper instances are created

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        sample_date : date
            Expected date of reference

        Returns
        -------
        None
        """
        assert instance.date_ref == sample_date
        assert instance.url == _EXPECTED_URL
        assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
        assert isinstance(instance.cls_dates_current, DatesCurrent)
        assert isinstance(instance.cls_create_log, CreateLog)
        assert isinstance(instance.cls_dates_br, DatesBRAnbima)
        assert isinstance(instance.cls_handling_dicts, HandlingDicts)

    def test_init_none_date_ref(self, mock_logger: MagicMock) -> None:
        """Test initialization with None date_ref falls back to previous working day.

        Parameters
        ----------
        mock_logger : MagicMock
            Mocked logger

        Returns
        -------
        None
        """
        with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2023, 1, 1)):
            inst = B3TheoricalPortfolioIBRA(date_ref=None, logger=mock_logger)
        assert inst.date_ref == date(2023, 1, 1)

    def test_get_response_success(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mock_requests_get: MagicMock,
        mock_response: MagicMock,
    ) -> None:
        """Test successful HTTP response retrieval.

        Verifies
        --------
        - requests.get is called with the correct URL and parameters
        - raise_for_status is invoked
        - Response object is returned as-is

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : MagicMock
            Mocked response object

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        result = instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
        assert result == mock_response
        mock_requests_get.assert_called_once_with(
            instance.url, timeout=(12.0, 21.0), verify=True
        )
        mock_response.raise_for_status.assert_called_once()

    def test_get_response_http_error(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mock_requests_get: MagicMock,
        mock_backoff: None,
    ) -> None:
        """Test that HTTPError propagates from get_response.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_backoff : None
            Backoff bypass fixture

        Returns
        -------
        None
        """
        from requests.exceptions import HTTPError

        mock_requests_get.return_value.raise_for_status.side_effect = HTTPError("404")
        with pytest.raises(HTTPError):
            instance.get_response()

    @pytest.mark.parametrize("timeout", [5, 10.5, (5.0, 10.0)])
    def test_get_response_timeout_variations(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mock_requests_get: MagicMock,
        mock_response: MagicMock,
        timeout: Union[int, float, tuple[float, float]],
    ) -> None:
        """Test get_response with different timeout values.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : MagicMock
            Mocked response object
        timeout : Union[int, float, tuple[float, float]]
            Timeout value under test

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        result = instance.get_response(timeout=timeout)
        assert result == mock_response
        mock_requests_get.assert_called_once_with(instance.url, timeout=timeout, verify=True)

    def test_parse_raw_file_valid(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mock_response: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        """Test parse_raw_file returns enriched list of dicts.

        Verifies
        --------
        - JSON is parsed and results extracted
        - Page and header data are merged into each record via HandlingDicts

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        mock_response : MagicMock
            Mocked response object
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        mocker.patch.object(
            instance.cls_handling_dicts,
            "add_key_value_to_dicts",
            side_effect=lambda list_ser, key: list_ser,
        )
        result = instance.parse_raw_file(mock_response)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, dict) for item in result)

    def test_parse_raw_file_invalid_json(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mocker: MockerFixture,
    ) -> None:
        """Test parse_raw_file propagates JSON decode errors.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        bad_response = MagicMock(spec=Response)
        bad_response.json.side_effect = ValueError("Invalid JSON")
        with pytest.raises(ValueError, match="Invalid JSON"):
            instance.parse_raw_file(bad_response)

    def test_transform_data_normal(self, instance: B3TheoricalPortfolioIBRA) -> None:
        """Test transform_data converts list of dicts to DataFrame.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance

        Returns
        -------
        None
        """
        input_data = [
            {"SEGMENT": "Stock", "COD": "PETR4", "PART": 5.2},
            {"SEGMENT": "Stock", "COD": "VALE3", "PART": 4.8},
        ]
        df_ = instance.transform_data(list_ser=input_data)
        assert isinstance(df_, pd.DataFrame)
        assert len(df_) == 2
        assert list(df_.columns) == ["SEGMENT", "COD", "PART"]

    def test_transform_data_empty(self, instance: B3TheoricalPortfolioIBRA) -> None:
        """Test transform_data returns empty DataFrame for empty input.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance

        Returns
        -------
        None
        """
        df_ = instance.transform_data(list_ser=[])
        assert isinstance(df_, pd.DataFrame)
        assert df_.empty

    def test_run_without_db(
        self,
        instance_no_db: B3TheoricalPortfolioIBRA,
        mock_requests_get: MagicMock,
        mock_response: MagicMock,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture,
    ) -> None:
        """Test run without database returns DataFrame.

        Parameters
        ----------
        instance_no_db : B3TheoricalPortfolioIBRA
            Initialized instance without database
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : MagicMock
            Mocked response object
        sample_dataframe : pd.DataFrame
            Sample DataFrame for patching
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        mocker.patch.object(instance_no_db, "transform_data", return_value=sample_dataframe)
        mocker.patch.object(instance_no_db, "standardize_dataframe", return_value=sample_dataframe)
        result = instance_no_db.run()
        assert isinstance(result, pd.DataFrame)
        assert result.equals(sample_dataframe)

    def test_run_with_db(
        self,
        instance: B3TheoricalPortfolioIBRA,
        mock_requests_get: MagicMock,
        mock_response: MagicMock,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture,
    ) -> None:
        """Test run with database calls insert_table_db and returns None.

        Parameters
        ----------
        instance : B3TheoricalPortfolioIBRA
            Initialized instance with database
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : MagicMock
            Mocked response object
        sample_dataframe : pd.DataFrame
            Sample DataFrame for patching
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        mocker.patch.object(instance, "transform_data", return_value=sample_dataframe)
        mocker.patch.object(instance, "standardize_dataframe", return_value=sample_dataframe)
        mocker.patch.object(instance, "insert_table_db")
        result = instance.run()
        assert result is None
        instance.insert_table_db.assert_called_once_with(
            cls_db=instance.cls_db,
            str_table_name="br_b3_theorical_portfolio_ibra",
            df_=sample_dataframe,
            bool_insert_or_ignore=False,
        )

    def test_run_invalid_table_name_raises(
        self,
        sample_date: date,
        mock_logger: MagicMock,
        mock_db_session: MagicMock,
        mock_requests_get: MagicMock,
        mock_response: MagicMock,
        sample_dataframe: pd.DataFrame,
        mocker: MockerFixture,
    ) -> None:
        """Test that an empty str_table_name with cls_db raises ValueError.

        Parameters
        ----------
        sample_date : date
            Sample date for initialization
        mock_logger : MagicMock
            Mocked logger
        mock_db_session : MagicMock
            Mocked database session
        mock_requests_get : MagicMock
            Mocked requests.get
        mock_response : MagicMock
            Mocked response object
        sample_dataframe : pd.DataFrame
            Sample DataFrame for patching
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks

        Returns
        -------
        None
        """
        inst = B3TheoricalPortfolioIBRA(
            date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
        )
        original = B3TheoricalPortfolioIBRA._TABLE_NAME
        B3TheoricalPortfolioIBRA._TABLE_NAME = ""
        try:
            mock_requests_get.return_value = mock_response
            mocker.patch.object(inst, "parse_raw_file", return_value=[])
            mocker.patch.object(inst, "standardize_dataframe", return_value=sample_dataframe)
            with pytest.raises(ValueError, match="str_table_name cannot be empty"):
                inst.run(str_table_name="")
        finally:
            B3TheoricalPortfolioIBRA._TABLE_NAME = original


# --------------------------
# Reload Tests
# --------------------------
def test_module_reload() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Class is accessible after reload

    Returns
    -------
    None
    """
    importlib.reload(
        sys.modules["stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibra"]
    )
    assert hasattr(
        sys.modules["stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibra"],
        "B3TheoricalPortfolioIBRA",
    )
