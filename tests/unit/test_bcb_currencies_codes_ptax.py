"""Unit tests for BCBCurrenciesCodesPTAX ingestion module.

Tests the ingestion functionality for BCB PTAX currency codes with various scenarios:
- Initialization with valid and invalid inputs
- Response handling and parsing
- Data transformation and standardization
- Database insertion and fallback logic
"""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_ptax import (
    BCBCurrenciesCodesPTAX,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


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
def mock_session(mocker: MockerFixture) -> Session:
    """Mock database Session object.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects.

    Returns
    -------
    Session
        Mocked database Session.
    """
    return mocker.MagicMock(spec=Session)


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        A fixed date for consistent testing.
    """
    return date(2025, 9, 17)


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> DatesCurrent:
    """Mock DatesCurrent for consistent date handling.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects.

    Returns
    -------
    DatesCurrent
        Mocked DatesCurrent.
    """
    mock = mocker.MagicMock(spec=DatesCurrent)
    mock.curr_date.return_value = date(2025, 9, 17)
    return mock


@pytest.fixture
def mock_dates_br(mocker: MockerFixture, sample_date: date) -> DatesBRAnbima:
    """Mock DatesBRAnbima for working days calculation.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker for creating mock objects.
    sample_date : date
        A fixed date for consistent testing.

    Returns
    -------
    DatesBRAnbima
        Mocked DatesBRAnbima.
    """
    mock = mocker.MagicMock(spec=DatesBRAnbima)
    mock.add_working_days.return_value = sample_date
    return mock


@pytest.fixture
def sample_csv_content() -> StringIO:
    """Provide sample CSV content for parsing.

    Returns
    -------
    StringIO
        A StringIO object with sample CSV content.
    """
    return StringIO(
        """CODIGO;NOME;SIMBOLO;CODIGO_PAIS;PAIS;TIPO;DATA_EXCLUSAO_PTAX
USD;Dolar Americano;US$;840;EUA;A;"""
    )


# --------------------------
# Tests for BCBCurrenciesCodesPTAX
# --------------------------
class TestBCBCurrenciesCodesPTAX:
    """Test cases for BCBCurrenciesCodesPTAX class."""

    @pytest.fixture
    def bcb_currencies_codes_ptax(
        self,
        mocker: MockerFixture,
        mock_dates_current: DatesCurrent,
        mock_dates_br: DatesBRAnbima,
    ) -> BCBCurrenciesCodesPTAX:
        """Fixture for BCBCurrenciesCodesPTAX instance.

        Parameters
        ----------
        mocker : MockerFixture
            Pytest mocker for creating mock objects.
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent.
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima.

        Returns
        -------
        BCBCurrenciesCodesPTAX
            Mocked BCBCurrenciesCodesPTAX.
        """
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DirFilesManagement"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.CreateLog"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.HtmlHandler"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesCurrent",
            return_value=mock_dates_current,
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesBRAnbima",
            return_value=mock_dates_br,
        )
        mocker.patch("requests.get")
        mocker.patch.object(BCBCurrenciesCodesPTAX, "get_file")
        mocker.patch.object(BCBCurrenciesCodesPTAX, "insert_table_db")
        mocker.patch.object(BCBCurrenciesCodesPTAX, "standardize_dataframe")
        return BCBCurrenciesCodesPTAX()

    def test_init_with_default_params(
        self,
        mocker: MockerFixture,
        mock_dates_current: DatesCurrent,
        mock_dates_br: DatesBRAnbima,
        sample_date: date,
    ) -> None:
        """Test initialization with default parameters.

        Parameters
        ----------
        mocker : MockerFixture
            Pytest mocker for creating mock objects.
        mock_dates_current : DatesCurrent
            Mocked DatesCurrent.
        mock_dates_br : DatesBRAnbima
            Mocked DatesBRAnbima.
        sample_date : date
            A fixed date for consistent testing.

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DirFilesManagement"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.CreateLog"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.HtmlHandler"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesCurrent",
            return_value=mock_dates_current,
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesBRAnbima",
            return_value=mock_dates_br,
        )

        instance = BCBCurrenciesCodesPTAX()
        assert instance.date_ref == sample_date
        assert instance.url == "https://www4.bcb.gov.br/Download/fechamento/M20250917.csv"
        assert isinstance(instance.logger, type(None))
        assert isinstance(instance.cls_db, type(None))
        mock_dates_br.add_working_days.assert_called_once()

    def test_init_with_custom_params(
        self,
        sample_date: date,
        mock_session: Session,
        mocker: MockerFixture,
    ) -> None:
        """Test initialization with custom parameters.

        Parameters
        ----------
        sample_date : date
            Sample date for testing.
        mock_session : Session
            Mocked database session.
        mocker : MockerFixture
            Pytest mocker for creating mock objects.

        Returns
        -------
        None
        """
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DirFilesManagement"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.CreateLog"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.HtmlHandler"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesCurrent"
        )
        mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics"
            ".bcb_currencies_codes_ptax.DatesBRAnbima"
        )

        logger = MagicMock(spec=Logger)
        instance = BCBCurrenciesCodesPTAX(
            date_ref=sample_date, logger=logger, cls_db=mock_session
        )
        assert instance.date_ref == sample_date
        assert instance.logger == logger
        assert instance.cls_db == mock_session

    def test_get_response_success(
        self,
        bcb_currencies_codes_ptax: BCBCurrenciesCodesPTAX,
        mock_response: Response,
        mocker: MockerFixture,
    ) -> None:
        """Test successful HTTP response retrieval.

        Parameters
        ----------
        bcb_currencies_codes_ptax : BCBCurrenciesCodesPTAX
            Instance of BCBCurrenciesCodesPTAX.
        mock_response : Response
            Mocked HTTP response.
        mocker : MockerFixture
            Pytest mocker for creating mock objects.

        Returns
        -------
        None
        """
        mocker.patch("requests.get", return_value=mock_response)
        result = bcb_currencies_codes_ptax.get_response(timeout=1000)
        assert result == mock_response
        mock_response.raise_for_status.assert_called_once()

    def test_parse_raw_file(
        self,
        bcb_currencies_codes_ptax: BCBCurrenciesCodesPTAX,
        mock_response: Response,
        mocker: MockerFixture,
    ) -> None:
        """Test parsing of raw file content.

        Parameters
        ----------
        bcb_currencies_codes_ptax : BCBCurrenciesCodesPTAX
            Instance of BCBCurrenciesCodesPTAX.
        mock_response : Response
            Mocked HTTP response.
        mocker : MockerFixture
            Pytest mocker for creating mock objects.

        Returns
        -------
        None
        """
        mock_file = StringIO("test content")
        mocker.patch.object(
            bcb_currencies_codes_ptax, "get_file", return_value=mock_file
        )
        result = bcb_currencies_codes_ptax.parse_raw_file(mock_response)
        assert result == mock_file

    def test_transform_data(
        self,
        bcb_currencies_codes_ptax: BCBCurrenciesCodesPTAX,
        sample_csv_content: StringIO,
    ) -> None:
        """Test transformation of CSV content to DataFrame.

        Parameters
        ----------
        bcb_currencies_codes_ptax : BCBCurrenciesCodesPTAX
            Instance of BCBCurrenciesCodesPTAX.
        sample_csv_content : StringIO
            Sample CSV content.

        Returns
        -------
        None
        """
        result = bcb_currencies_codes_ptax.transform_data(sample_csv_content)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            "CODIGO", "NOME", "SIMBOLO", "CODIGO_PAIS", "PAIS", "TIPO",
            "DATA_EXCLUSAO_PTAX",
        ]
        assert len(result) == 1
        assert result.iloc[0]["CODIGO"] == "USD"

    def test_run_without_db(
        self,
        bcb_currencies_codes_ptax: BCBCurrenciesCodesPTAX,
        mock_response: Response,
        sample_csv_content: StringIO,
        mocker: MockerFixture,
    ) -> None:
        """Test run method without database session.

        Parameters
        ----------
        bcb_currencies_codes_ptax : BCBCurrenciesCodesPTAX
            Instance of BCBCurrenciesCodesPTAX.
        mock_response : Response
            Mocked HTTP response.
        sample_csv_content : StringIO
            Sample CSV content.
        mocker : MockerFixture
            Pytest mocker for creating mock objects.

        Returns
        -------
        None
        """
        mocker.patch.object(
            bcb_currencies_codes_ptax, "get_response", return_value=mock_response
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "parse_raw_file", return_value=sample_csv_content
        )
        mock_df = pd.read_csv(
            sample_csv_content,
            sep=";",
            skiprows=1,
            names=["CODIGO", "NOME", "SIMBOLO", "CODIGO_PAIS", "PAIS", "TIPO",
                   "DATA_EXCLUSAO_PTAX"],
            header=None,
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "transform_data", return_value=mock_df
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "standardize_dataframe", return_value=mock_df
        )

        result = bcb_currencies_codes_ptax.run(timeout=1000)
        assert isinstance(result, pd.DataFrame)
        bcb_currencies_codes_ptax.standardize_dataframe.assert_called_once()

    def test_run_with_db(
        self,
        bcb_currencies_codes_ptax: BCBCurrenciesCodesPTAX,
        mock_response: Response,
        sample_csv_content: StringIO,
        mock_session: Session,
        mocker: MockerFixture,
    ) -> None:
        """Test run method with database session.

        Parameters
        ----------
        bcb_currencies_codes_ptax : BCBCurrenciesCodesPTAX
            Instance of BCBCurrenciesCodesPTAX.
        mock_response : Response
            Mocked HTTP response.
        sample_csv_content : StringIO
            Sample CSV content.
        mock_session : Session
            Mocked database Session.
        mocker : MockerFixture
            Pytest mocker for creating mock objects.

        Returns
        -------
        None
        """
        bcb_currencies_codes_ptax.cls_db = mock_session
        mocker.patch.object(
            bcb_currencies_codes_ptax, "get_response", return_value=mock_response
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "parse_raw_file", return_value=sample_csv_content
        )
        mock_df = pd.read_csv(
            sample_csv_content,
            sep=";",
            skiprows=1,
            names=["CODIGO", "NOME", "SIMBOLO", "CODIGO_PAIS", "PAIS", "TIPO",
                   "DATA_EXCLUSAO_PTAX"],
            header=None,
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "transform_data", return_value=mock_df
        )
        mocker.patch.object(
            bcb_currencies_codes_ptax, "standardize_dataframe", return_value=mock_df
        )

        result = bcb_currencies_codes_ptax.run(timeout=1000)
        assert result is None
        bcb_currencies_codes_ptax.insert_table_db.assert_called_once()
