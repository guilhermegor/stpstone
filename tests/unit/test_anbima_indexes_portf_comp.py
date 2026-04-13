"""Unit tests for AnbimaIndexesPortfComp class."""

from datetime import date
from io import StringIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.exchange.anbima_indexes_portf_comp import (
    AnbimaIndexesPortfComp,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


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
    response.content = b"##Header\nLine1\nLine2\n\nLine3"
    response.text = "##Header\nLine1\nLine2\n\nLine3"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_session() -> Session:
    """Mock database session with insert method.

    Returns
    -------
    Session
        Mocked database session.
    """
    session = MagicMock(spec=Session)
    session.insert = MagicMock()
    return session


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date.

    Returns
    -------
    date
        A fixed date for consistent testing.
    """
    return date(2025, 9, 8)


@pytest.fixture
def mock_requests_get(mocker: MockerFixture, mock_response: Response) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.
    mock_response : Response
        Mocked response object.

    Returns
    -------
    MagicMock
        Mocked requests.get object.
    """
    return mocker.patch("requests.get", return_value=mock_response)


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
    """Mock backoff to bypass retry delays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    MagicMock
        Mocked backoff.on_exception object.
    """
    return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def anbima_portf(sample_date: date, mock_session: Session) -> AnbimaIndexesPortfComp:
    """Fixture providing AnbimaIndexesPortfComp instance.

    Parameters
    ----------
    sample_date : date
        Sample date for initialization.
    mock_session : Session
        Mocked database session.

    Returns
    -------
    AnbimaIndexesPortfComp
        Initialized instance.
    """
    return AnbimaIndexesPortfComp(date_ref=sample_date, cls_db=mock_session)


# --------------------------
# Tests for AnbimaIndexesPortfComp
# --------------------------
class TestAnbimaIndexesPortfComp:
    """Test cases for AnbimaIndexesPortfComp class."""

    def test_init_with_valid_inputs(self, sample_date: date, mock_session: Session) -> None:
        """Test initialization with valid inputs.

        Verifies
        --------
        - Instance is properly initialized with provided date.
        - Logger and database session are set.
        - URL is correctly set.

        Parameters
        ----------
        sample_date : date
            Sample date used for initialization.
        mock_session : Session
            Mocked database session.

        Returns
        -------
        None
        """
        instance = AnbimaIndexesPortfComp(date_ref=sample_date, cls_db=mock_session)
        assert instance.date_ref == sample_date
        assert instance.cls_db == mock_session
        assert instance.url == "https://www.anbima.com.br/informacoes/ima/arqs/ima_completo.txt"
        assert isinstance(instance.cls_dates_br, DatesBRAnbima)
        assert isinstance(instance.cls_dates_current, DatesCurrent)

    def test_init_default_date(self, mock_session: Session, mocker: MockerFixture) -> None:
        """Test initialization with default date.

        Verifies
        --------
        - Instance is properly initialized with default date.
        - Logger and database session are set.
        - URL is correctly set.

        Parameters
        ----------
        mock_session : Session
            Mocked database session.
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks.

        Returns
        -------
        None
        """
        mock_date = mocker.patch.object(DatesCurrent, "curr_date", return_value=date(2025, 9, 8))
        mock_prev_day = mocker.patch.object(
            DatesBRAnbima, "add_working_days", return_value=date(2025, 9, 5)
        )
        instance = AnbimaIndexesPortfComp(cls_db=mock_session)
        assert instance.date_ref == date(2025, 9, 5)
        mock_date.assert_called_once()
        mock_prev_day.assert_called_once_with(date(2025, 9, 8), -1)

    def test_get_response_success(
        self,
        anbima_portf: AnbimaIndexesPortfComp,
        mock_requests_get: MagicMock,
        mock_backoff: MagicMock,
        mock_response: Response,
    ) -> None:
        """Test successful HTTP response retrieval.

        Verifies
        --------
        - HTTP request is made with correct parameters.
        - Response is returned as expected.
        - No HTTP errors are raised.

        Parameters
        ----------
        anbima_portf : AnbimaIndexesPortfComp
            Initialized instance.
        mock_requests_get : MagicMock
            Mocked requests.get.
        mock_backoff : MagicMock
            Mocked backoff decorator.
        mock_response : Response
            Mocked response object.

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response
        result = anbima_portf.get_response(timeout=(12.0, 21.0), bool_verify=True)
        assert result == mock_response
        mock_requests_get.assert_called_once_with(
            anbima_portf.url, timeout=(12.0, 21.0), verify=True
        )
        mock_response.raise_for_status.assert_called_once()

    def test_parse_raw_file(
        self,
        anbima_portf: AnbimaIndexesPortfComp,
        mock_response: Response,
        mocker: MockerFixture,
    ) -> None:
        """Test parsing of raw file content.

        Verifies
        --------
        - get_file is called and returns StringIO.
        - Correct content is returned.

        Parameters
        ----------
        anbima_portf : AnbimaIndexesPortfComp
            Initialized instance.
        mock_response : Response
            Mocked response object.
        mocker : MockerFixture
            Pytest-mock fixture.

        Returns
        -------
        None
        """
        mocker.patch.object(
            anbima_portf,
            "get_file",
            return_value=StringIO("##Header\nLine1\n\nLine2\nLine3\n"),
        )
        result = anbima_portf.parse_raw_file(mock_response)
        assert isinstance(result, StringIO)
        assert result.getvalue() == "Line2\nLine3"

    def test_transform_data(
        self, anbima_portf: AnbimaIndexesPortfComp, mocker: MockerFixture
    ) -> None:
        """Test data transformation to DataFrame.

        Verifies
        --------
        - Correct DataFrame is returned.

        Parameters
        ----------
        anbima_portf : AnbimaIndexesPortfComp
            Initialized instance.
        mocker : MockerFixture
            Pytest-mock fixture.

        Returns
        -------
        None
        """
        sample_data = StringIO(
            "01/01/2025@IMA@Bond@01/01/2030@123@ISIN@1.2@100@200@1000@2000"
            "@3000@0.5@250@300@5@100@2000@0.7@0.8"
        )
        mock_read_csv = mocker.patch(
            "pandas.read_csv",
            return_value=pd.DataFrame({"DATA_REFERENCIA": ["01/01/2025"]}),
        )
        result = anbima_portf.transform_data(sample_data)
        assert isinstance(result, pd.DataFrame)
        mock_read_csv.assert_called_once_with(
            sample_data,
            sep="@",
            skiprows=2,
            engine="python",
            decimal=",",
            na_values="--",
            encoding="latin-1",
            on_bad_lines="skip",
            names=[
                "DATA_REFERENCIA",
                "INDICE",
                "TITULOS",
                "DATA_VENCIMENTO",
                "CODIGO_SELIC",
                "CODIGO_ISIN",
                "TAXA_INDICATIVA",
                "PU",
                "PU_JUROS",
                "QUANTIDADE_1000_TITULOS",
                "QUANTIDADE_TEORICA_1000_TITULOS",
                "CARTEIRA_A_MERCADO_RS_MIL",
                "PESO",
                "PRAZO_DU",
                "DURATION_DU",
                "NUMERO_OPERACOES",
                "QTD_NEGOCIADA_1000_TITULOS",
                "VALOR_NEGOCIADO_RS_MIL",
                "PMR",
                "CONVEXIDADE",
            ],
        )

    def test_run_without_db(
        self,
        anbima_portf: AnbimaIndexesPortfComp,
        mocker: MockerFixture,
        mock_response: Response,
    ) -> None:
        """Test run method without database session.

        Verifies
        --------
        - Full ingestion process without DB insertion.
        - Correct DataFrame is returned.
        - All intermediate methods are called.

        Parameters
        ----------
        anbima_portf : AnbimaIndexesPortfComp
            Initialized instance.
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks.
        mock_response : Response
            Mocked response object.

        Returns
        -------
        None
        """
        anbima_portf.cls_db = None

        mocker.patch.object(anbima_portf, "get_response", return_value=mock_response)
        mock_file = StringIO("test")
        mocker.patch.object(anbima_portf, "parse_raw_file", return_value=mock_file)
        mock_df = pd.DataFrame({"DATA_REFERENCIA": ["01/01/2025"]})
        mocker.patch.object(anbima_portf, "transform_data", return_value=mock_df)
        mock_standardize = mocker.patch.object(
            anbima_portf, "standardize_dataframe", return_value=mock_df
        )

        result = anbima_portf.run(timeout=10, bool_verify=False, bool_insert_or_ignore=True)
        assert isinstance(result, pd.DataFrame)
        mock_standardize.assert_called_once_with(
            df_=mock_df,
            date_ref=anbima_portf.date_ref,
            dict_dtypes={
                "DATA_REFERENCIA": "date",
                "INDICE": str,
                "TITULOS": str,
                "DATA_VENCIMENTO": "date",
                "CODIGO_SELIC": int,
                "CODIGO_ISIN": str,
                "TAXA_INDICATIVA": float,
                "PU": float,
                "PU_JUROS": float,
                "QUANTIDADE_1000_TITULOS": int,
                "QUANTIDADE_TEORICA_1000_TITULOS": int,
                "CARTEIRA_A_MERCADO_RS_MIL": float,
                "PESO": float,
                "PRAZO_DU": int,
                "DURATION_DU": float,
                "NUMERO_OPERACOES": int,
                "QTD_NEGOCIADA_1000_TITULOS": int,
                "VALOR_NEGOCIADO_RS_MIL": float,
                "PMR": float,
                "CONVEXIDADE": float,
            },
            str_fmt_dt="DD/MM/YYYY",
            url=anbima_portf.url,
        )

    def test_run_with_db(
        self,
        anbima_portf: AnbimaIndexesPortfComp,
        mocker: MockerFixture,
        mock_response: Response,
    ) -> None:
        """Test run method with database session.

        Verifies
        --------
        - Full ingestion process with DB insertion.
        - Database insertion method is called.
        - No DataFrame is returned.

        Parameters
        ----------
        anbima_portf : AnbimaIndexesPortfComp
            Initialized instance.
        mocker : MockerFixture
            Pytest-mock fixture for creating mocks.
        mock_response : Response
            Mocked response object.

        Returns
        -------
        None
        """
        mocker.patch.object(anbima_portf, "get_response", return_value=mock_response)
        mock_file = StringIO("test")
        mocker.patch.object(anbima_portf, "parse_raw_file", return_value=mock_file)
        mock_df = pd.DataFrame({"DATA_REFERENCIA": ["01/01/2025"]})
        mocker.patch.object(anbima_portf, "transform_data", return_value=mock_df)
        mocker.patch.object(anbima_portf, "standardize_dataframe", return_value=mock_df)
        mock_insert = mocker.patch.object(anbima_portf, "insert_table_db")

        result = anbima_portf.run(str_table_name="test_table")
        assert result is None
        mock_insert.assert_called_once_with(
            cls_db=anbima_portf.cls_db,
            str_table_name="test_table",
            df_=mock_df,
            bool_insert_or_ignore=False,
        )
