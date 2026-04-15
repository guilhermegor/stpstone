"""Unit tests for BCBOlindaCurrencies ingestion class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.abc.ingestion_abc import Session
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies import (
    BCBOlindaCurrencies,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_logger() -> Logger:
    """Fixture providing a mock logger instance.

    Returns
    -------
    Logger
        Mocked logger object for testing.
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
    """Fixture providing a mock database session.

    Returns
    -------
    Session
        Mocked database session object.
    """
    return MagicMock(spec=Session)


@pytest.fixture
def mock_response_success() -> Response:
    """Fixture providing a successful HTTP response mock.

    Returns
    -------
    Response
        Mocked Response object with successful status and JSON data.
    """
    mock_resp = MagicMock(spec=Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "value": [
            {
                "simbolo": "USD",
                "nomeFormatado": "Dólar americano",
                "tipoMoeda": "Real",
            },
            {
                "simbolo": "EUR",
                "nomeFormatado": "Euro",
                "tipoMoeda": "Real",
            },
        ]
    }
    return mock_resp


@pytest.fixture
def mock_dates_current(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the DatesCurrent class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked DatesCurrent instance.
    """
    mock_dates = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies.DatesCurrent"
    )
    mock_instance = MagicMock(spec=DatesCurrent)
    mock_dates.return_value = mock_instance
    mock_instance.curr_date.return_value = date(2025, 9, 16)
    return mock_instance


@pytest.fixture
def mock_dates_br(mocker: MockerFixture, mock_dates_current: MagicMock) -> MagicMock:
    """Fixture mocking the DatesBRAnbima class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.
    mock_dates_current : MagicMock
        Mocked DatesCurrent instance.

    Returns
    -------
    MagicMock
        Mocked DatesBRAnbima instance.
    """
    mock_br = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies.DatesBRAnbima"
    )
    mock_instance = MagicMock(spec=DatesBRAnbima)
    mock_br.return_value = mock_instance
    mock_instance.add_working_days.return_value = date(2025, 9, 15)
    return mock_instance


@pytest.fixture
def mock_dir_files(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the DirFilesManagement class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked DirFilesManagement instance.
    """
    mock_files = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies.DirFilesManagement"
    )
    mock_instance = MagicMock(spec=DirFilesManagement)
    mock_files.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the CreateLog class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked CreateLog instance.
    """
    mock_log = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies.CreateLog"
    )
    mock_instance = MagicMock(spec=CreateLog)
    mock_log.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked requests.get function.
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking backoff decorator to bypass retries.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked backoff.on_exception decorator.
    """
    return mocker.patch("backoff.on_exception", return_value=lambda func: func)


@pytest.fixture
def mock_insert_table_db(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the insert_table_db method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture.

    Returns
    -------
    MagicMock
        Mocked insert_table_db method.
    """
    return mocker.patch(
        "stpstone.ingestion.abc.ingestion_abc.ABCIngestionOperations.insert_table_db"
    )


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date for testing.

    Returns
    -------
    date
        A sample date (2025-09-15).
    """
    return date(2025, 9, 15)


# --------------------------
# Tests
# --------------------------


class TestBCBOlindaCurrencies:
    """Test cases for BCBOlindaCurrencies class."""

    def test_init_with_valid_inputs(
        self,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test initialization with valid parameters.

        Parameters
        ----------
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance.
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance.
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance.
        mock_create_log : MagicMock
            Mocked CreateLog instance.

        Returns
        -------
        None
        """
        test_date = date(2025, 9, 14)
        mock_logger = MagicMock(spec=Logger)
        mock_session = MagicMock(spec=Session)

        instance = BCBOlindaCurrencies(
            date_ref=test_date,
            logger=mock_logger,
            cls_db=mock_session,
        )

        assert instance.date_ref == test_date
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_session
        assert instance.url == (
            "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            "Moedas?$top=100&$format=json&$select=simbolo,nomeFormatado,tipoMoeda"
        )

    def test_run_successful_http_no_db(
        self,
        mock_requests_get: MagicMock,
        mock_response_success: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        """Test successful run without database session returns DataFrame.

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get function.
        mock_response_success : Response
            Mocked successful HTTP response.
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance.
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance.
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance.
        mock_create_log : MagicMock
            Mocked CreateLog instance.
        mocker : MockerFixture
            Pytest mocker fixture.

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response_success

        standardized_df = pd.DataFrame({
            "SIMBOLO": ["USD", "EUR"],
            "NOME_FORMATADO": ["Dólar americano", "Euro"],
            "TIPO_MOEDA": ["Real", "Real"],
        })

        instance = BCBOlindaCurrencies()
        mock_standardize = mocker.patch.object(
            instance, "standardize_dataframe", return_value=standardized_df
        )

        result_df = instance.run(timeout=10.0, bool_verify=False)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2
        assert set(result_df.columns) == {"SIMBOLO", "NOME_FORMATADO", "TIPO_MOEDA"}
        mock_requests_get.assert_called_once_with(
            instance.url,
            timeout=10.0,
            verify=False,
        )
        mock_standardize.assert_called_once()

    def test_get_response_success(
        self,
        mock_requests_get: MagicMock,
        mock_response_success: Response,
        mock_backoff: MagicMock,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test get_response method successful HTTP request.

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get function.
        mock_response_success : Response
            Mocked successful HTTP response.
        mock_backoff : MagicMock
            Mocked backoff function.
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance.
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance.
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance.
        mock_create_log : MagicMock
            Mocked CreateLog instance.

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response_success

        instance = BCBOlindaCurrencies()
        result = instance.get_response(timeout=15.0, bool_verify=True)

        assert result == mock_response_success
        mock_requests_get.assert_called_once_with(
            instance.url,
            timeout=15.0,
            verify=True,
        )
        mock_response_success.raise_for_status.assert_called_once()

    def test_parse_raw_file_valid_json(
        self,
        mock_response_success: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test parse_raw_file method with valid JSON response.

        Parameters
        ----------
        mock_response_success : Response
            Mocked successful HTTP response.
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance.
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance.
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance.
        mock_create_log : MagicMock
            Mocked CreateLog instance.

        Returns
        -------
        None
        """
        instance = BCBOlindaCurrencies()
        result = instance.parse_raw_file(mock_response_success)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["simbolo"] == "USD"
        assert result[1]["simbolo"] == "EUR"
        mock_response_success.json.assert_called_once()

    def test_transform_data_valid_json(
        self,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test transform_data method with valid JSON list.

        Parameters
        ----------
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance.
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance.
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance.
        mock_create_log : MagicMock
            Mocked CreateLog instance.

        Returns
        -------
        None
        """
        sample_json = [
            {"simbolo": "USD", "nomeFormatado": "Dólar", "tipoMoeda": "Real"},
            {"simbolo": "EUR", "nomeFormatado": "Euro", "tipoMoeda": "Real"},
        ]

        instance = BCBOlindaCurrencies()
        result_df = instance.transform_data(json_=sample_json)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 2
        assert len(result_df.columns) == 3
        assert set(result_df.columns) == {"simbolo", "nomeFormatado", "tipoMoeda"}
        pd.testing.assert_frame_equal(
            result_df,
            pd.DataFrame(sample_json),
        )
