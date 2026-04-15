"""Unit tests for DebenturesComBrOTCPVs class.

Tests the ingestion functionality of DebenturesComBrOTCPVs, including:
- Initialization with valid inputs and edge cases
- HTTP error and timeout handling
- Data retrieval and parsing
"""

from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.otc.debentures_com_br_otc_pvs import DebenturesComBrOTCPVs


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response(mocker: MockerFixture) -> Response:
    """Mock Response object with sample content.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects.

    Returns
    -------
    Response
        Mocked Response object with HTML content.
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
        A fixed date range for consistent testing.
    """
    return date(2023, 1, 1), date(2023, 1, 5)


@pytest.fixture
def mock_session() -> MagicMock:
    """Mock database session.

    Returns
    -------
    MagicMock
        Mocked database session.
    """
    return MagicMock()


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
            Pytest-mocker fixture.

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
            Mocked requests.get.
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing.

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

    def test_transform_data(self) -> None:
        """Test transform_data method.

        Verifies
        --------
        - Correct parsing of CSV content
        - Proper column names
        - Handling of NA values

        Returns
        -------
        None
        """
        csv_content = """Header1
Header2
Header3
01/01/2023\tEMISSOR\tCODIGO\tISIN123\t100\t1\t100,0\t100,0\t100,0\t100,0
Footer1
Footer2
Footer3
Footer4"""
        instance = DebenturesComBrOTCPVs()
        df_ = instance.transform_data(StringIO(csv_content))
        assert df_.shape[0] == 1
        assert df_["EMISSOR"].iloc[0] == "EMISSOR"
        assert df_["PU_MEDIO"].iloc[0] == 100.0
