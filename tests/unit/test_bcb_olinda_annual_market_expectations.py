"""Unit tests for BCBOlindaAnnualMarketExpectations ingestion class."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_annual_market_expectations import (
    BCBOlindaAnnualMarketExpectations,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_response_expectations() -> Response:
    """Fixture providing successful market expectations response mock.

    Returns
    -------
    Response
        Mocked Response object with expectations data.
    """
    mock_resp = MagicMock(spec=Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "value": [
            {
                "Indicador": "IPCA",
                "IndicadorDetalhe": "IPCA",
                "Data": "2025-09-15",
                "DataReferencia": 202509,
                "Media": 4.5,
                "Mediana": 4.4,
                "Minimo": 4.0,
                "Maximo": 5.0,
                "numeroRespondentes": 45,
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
        "stpstone.ingestion.countries.br.macroeconomics"
        ".bcb_olinda_annual_market_expectations.DatesCurrent"
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
        "stpstone.ingestion.countries.br.macroeconomics"
        ".bcb_olinda_annual_market_expectations.DatesBRAnbima"
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
        "stpstone.ingestion.countries.br.macroeconomics"
        ".bcb_olinda_annual_market_expectations.DirFilesManagement"
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
        "stpstone.ingestion.countries.br.macroeconomics"
        ".bcb_olinda_annual_market_expectations.CreateLog"
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


class TestBCBOlindaAnnualMarketExpectations:
    """Test cases for BCBOlindaAnnualMarketExpectations class."""

    def test_init_with_valid_inputs(
        self,
        sample_date: date,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test initialization with valid reference date.

        Parameters
        ----------
        sample_date : date
            Sample date for testing.
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
        instance = BCBOlindaAnnualMarketExpectations(date_ref=sample_date)

        assert instance.date_ref == sample_date
        assert "$top=100000" in instance.url
        assert "$orderby=Data%20desc" in instance.url
        assert "ExpectativasMercadoAnuais" in instance.url

    def test_run_successful_expectations_data(
        self,
        mock_requests_get: MagicMock,
        mock_response_expectations: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        """Test successful market expectations data ingestion.

        Parameters
        ----------
        mock_requests_get : MagicMock
            Mocked requests.get method.
        mock_response_expectations : Response
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
        mock_requests_get.return_value = mock_response_expectations

        standardized_df = pd.DataFrame({
            "INDICADOR": ["IPCA"],
            "INDICADOR_DETALHE": ["IPCA"],
            "DATA": [date(2025, 9, 15)],
            "DATA_REFERENCIA": [202509],
            "MEDIA": [4.5],
            "MEDIANA": [4.4],
            "MINIMO": [4.0],
            "MAXIMO": [5.0],
            "NUMERO_RESPONDENTES": [45],
        })

        instance = BCBOlindaAnnualMarketExpectations()
        _ = mocker.patch.object(instance, "standardize_dataframe", return_value=standardized_df)

        result_df = instance.run(timeout=20.0)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        expected_columns = {
            "INDICADOR",
            "INDICADOR_DETALHE",
            "DATA",
            "DATA_REFERENCIA",
            "MEDIA",
            "MEDIANA",
            "MINIMO",
            "MAXIMO",
            "NUMERO_RESPONDENTES",
        }
        assert set(result_df.columns) == expected_columns

    def test_parse_raw_file_expectations_format(
        self,
        mock_response_expectations: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test parse_raw_file method for expectations data structure.

        Parameters
        ----------
        mock_response_expectations : Response
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
        instance = BCBOlindaAnnualMarketExpectations()
        result = instance.parse_raw_file(mock_response_expectations)

        assert isinstance(result, list)
        assert len(result) == 1
        record = result[0]
        fields = [
            "Indicador",
            "IndicadorDetalhe",
            "Data",
            "DataReferencia",
            "Media",
            "Mediana",
            "Minimo",
            "Maximo",
            "numeroRespondentes",
        ]
        for field in fields:
            assert field in record

        assert isinstance(record["Media"], (int, float))
        assert isinstance(record["DataReferencia"], int)
        assert isinstance(record["numeroRespondentes"], int)
        assert "2025-09-15" in record["Data"]
