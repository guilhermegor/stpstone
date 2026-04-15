"""Unit tests for DebenturesComBrPrimaryOffers class.

Tests the ingestion functionality of DebenturesComBrPrimaryOffers, including:
- URL date formatting
- Data transformation
- Error handling
"""

from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.otc.debentures_com_br_primary_offers import (
    DebenturesComBrPrimaryOffers,
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
def mock_stringio_primary_offers(mocker: MockerFixture) -> StringIO:
    """Mock StringIO object with sample CSV content for Primary Offers.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects.

    Returns
    -------
    StringIO
        A StringIO object with sample CSV content.
    """
    csv_content = """Header1
Header2
Header3
TEST\tCOMPANY\tACTIVE\t01/01/2023\t01/01/2023\t01/01/2023\t1000,0
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
        Mocked database session.
    """
    return MagicMock()


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
            A fixed date range for consistent testing.

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
            assert "01%2F01%2F2023" in called_url
            assert "05%2F01%2F2023" in called_url

    def test_transform_data_primary_offers(
        self, mock_stringio_primary_offers: StringIO
    ) -> None:
        """Test transform_data for PrimaryOffers class.

        Verifies
        --------
        - Correct parsing of CSV content
        - Proper column names and data types

        Parameters
        ----------
        mock_stringio_primary_offers : StringIO
            Mocked StringIO object.

        Returns
        -------
        None
        """
        instance = DebenturesComBrPrimaryOffers()
        df_ = instance.transform_data(mock_stringio_primary_offers)
        assert df_.shape[0] == 1
        assert df_["CODIGO_ATIVO"].iloc[0] == "TEST"
        assert df_["VOLUME_MOEDA_EPOCA"].iloc[0] == 1000.0
