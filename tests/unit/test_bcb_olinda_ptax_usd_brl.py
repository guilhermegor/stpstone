"""Unit tests for BCBOlindaPTAXUSDBRL ingestion class."""

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.abc.ingestion_abc import Session
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl import (
    BCBOlindaPTAXUSDBRL,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_response_ptax() -> Response:
    """Fixture providing a successful PTAX response mock.

    Returns
    -------
    Response
        Mocked Response object with PTAX data.
    """
    mock_resp = MagicMock(spec=Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "value": [
            {
                "cotacaoCompra": 5.12,
                "cotacaoVenda": 5.15,
                "dataHoraCotacao": "2025-09-15T13:00:00",
            }
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
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl.DatesCurrent"
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
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl.DatesBRAnbima"
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
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl.DirFilesManagement"
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
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl.CreateLog"
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
def sample_date_range() -> tuple[date, date]:
    """Fixture providing sample date range for testing.

    Returns
    -------
    tuple[date, date]
        Sample start and end dates (2025-08-17 to 2025-09-15).
    """
    return date(2025, 8, 17), date(2025, 9, 15)


# --------------------------
# Tests
# --------------------------


class TestBCBOlindaPTAXUSDBRL:
    """Test cases for BCBOlindaPTAXUSDBRL class."""

    def test_init_with_valid_inputs(
        self,
        sample_date_range: tuple[date, date],
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test initialization with valid date range parameters.

        Parameters
        ----------
        sample_date_range : tuple[date, date]
            Sample date range tuple.
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
        date_start, date_end = sample_date_range
        mock_logger = MagicMock(spec=Logger)
        mock_session = MagicMock(spec=Session)

        instance = BCBOlindaPTAXUSDBRL(
            date_start=date_start,
            date_end=date_end,
            logger=mock_logger,
            cls_db=mock_session,
        )

        assert instance.date_start == date_start
        assert instance.date_end == date_end
        assert instance.logger == mock_logger
        assert instance.cls_db == mock_session

    def test_run_successful_ptax_data(
        self,
        mock_requests_get: MagicMock,
        mock_response_ptax: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        """Test successful PTAX data ingestion without database.

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get method.
        mock_response_ptax : Response
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
        mock_requests_get.return_value = mock_response_ptax

        standardized_df = pd.DataFrame({
            "COTACAO_COMPRA": [5.12],
            "COTACAO_VENDA": [5.15],
            "DATA_HORA_COTACAO": ["2025-09-15T13:00:00"],
        })

        instance = BCBOlindaPTAXUSDBRL()
        _ = mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)

        result_df = instance.run(timeout=10.0, bool_verify=False)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert set(result_df.columns) == {
            "COTACAO_COMPRA",
            "COTACAO_VENDA",
            "DATA_HORA_COTACAO",
        }

        call_args = mock_requests_get.call_args
        assert call_args.kwargs["timeout"] == 10.0
        assert call_args.kwargs["verify"] is False

    def test_parse_raw_file_ptax_format(
        self,
        mock_response_ptax: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test parse_raw_file method with PTAX-specific data structure.

        Parameters
        ----------
        mock_response_ptax : Response
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
        instance = BCBOlindaPTAXUSDBRL()
        result = instance.parse_raw_file(mock_response_ptax)

        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        record = result[0]
        assert "cotacaoCompra" in record
        assert "cotacaoVenda" in record
        assert "dataHoraCotacao" in record
        assert record["cotacaoCompra"] == 5.12
        assert record["cotacaoVenda"] == 5.15
        assert "2025-09-15" in record["dataHoraCotacao"]
