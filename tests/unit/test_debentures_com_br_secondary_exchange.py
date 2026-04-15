"""Unit tests for DebenturesComBrSecondaryExchange class.

Tests the ingestion functionality of DebenturesComBrSecondaryExchange, including:
- Run method with and without database session
- Data transformation and edge cases
- NA value handling
"""

from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.otc.debentures_com_br_secondary_exchange import (
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
            Mocked requests.get response.
        sample_dates : tuple[date, date]
            A fixed date range for consistent testing.

        Returns
        -------
        None
        """
        mock_stringio = StringIO("""Header1
Header2
Header3
01/01/2023\tEMISSOR\tCODIGO\tISIN123\t100\t1\t100.0\t100.0\t100.0\t100.0
Footer1
Footer2
Footer3""")

        instance = DebenturesComBrSecondaryExchange(
            date_start=sample_dates[0], date_end=sample_dates[1]
        )
        with patch("requests.get", return_value=mock_response), \
                patch.object(instance, "get_file", return_value=mock_stringio), \
                patch.object(instance, "standardize_dataframe") as mock_standardize:
            expected_df = pd.DataFrame({"test": ["data"]})
            mock_standardize.return_value = expected_df

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
            Pytest-mocker fixture.

        Returns
        -------
        None
        """
        csv_content = """Header1
Header2
Header3
-\t-\t-\t-\t-\t-\t-\t-\t-\t-
Footer1
Footer2
Footer3"""
        mock_stringio = StringIO(csv_content)
        instance = DebenturesComBrSecondaryExchange()
        df_ = instance.transform_data(mock_stringio)
        assert isinstance(df_, pd.DataFrame)
        assert df_.shape[0] == 1
        assert df_.isna().all().all()
