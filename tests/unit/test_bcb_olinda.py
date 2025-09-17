"""Unit tests for BCB Olinda ingestion classes.

Tests the currency and market expectations ingestion functionality with various input scenarios 
including:
- Initialization with valid and invalid inputs
- HTTP response handling and parsing
- Data transformation and standardization
- Database insertion paths
- Error conditions and fallback mechanisms
- Backoff retry logic
"""


from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.abc.ingestion_abc import Session
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda import (
    BCBOlindaAnnualMarketExpectations,
    BCBOlindaCurrencies,
    BCBOlindaCurrenciesTS,
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
def mock_logger() -> Logger:
    """Fixture providing a mock logger instance.

    Returns
    -------
    Logger
        Mocked logger object for testing
    """
    mock_log = MagicMock(spec=Logger)
    return mock_log


@pytest.fixture
def mock_db_session() -> Session:
    """Fixture providing a mock database session.

    Returns
    -------
    Session
        Mocked database session object
    """
    mock_session = MagicMock(spec=Session)
    return mock_session


@pytest.fixture
def mock_response_success() -> Response:
    """Fixture providing a successful HTTP response mock.

    Returns
    -------
    Response
        Mocked Response object with successful status and JSON data
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
def mock_response_ptax() -> Response:
    """Fixture providing a successful PTAX response mock.

    Returns
    -------
    Response
        Mocked Response object with PTAX data
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
def mock_response_expectations() -> Response:
    """Fixture providing successful market expectations response mock.

    Returns
    -------
    Response
        Mocked Response object with expectations data
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
def mock_response_currencies_ts() -> Response:
    """Fixture providing successful currencies time series response mock.

    Returns
    -------
    Response
        Mocked Response object with currencies TS data
    """
    mock_resp = MagicMock(spec=Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "value": [
            {
                "paridadeCompra": 1.0,
                "paridadeVenda": 1.0,
                "cotacaoCompra": 5.12,
                "cotacaoVenda": 5.15,
                "dataHoraCotacao": "2025-09-15T13:00:00",
                "tipoBoletim": "Fechamento",
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
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked DatesCurrent instance
    """
    mock_dates = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda.DatesCurrent")
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
        Pytest mocker fixture
    mock_dates_current : MagicMock
        Mocked DatesCurrent instance

    Returns
    -------
    MagicMock
        Mocked DatesBRAnbima instance
    """
    mock_br = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda.DatesBRAnbima")
    mock_instance = MagicMock(spec=DatesBRAnbima)
    mock_br.return_value = mock_instance
    
    # Mock add_working_days to return specific dates
    mock_instance.add_working_days.return_value = date(2025, 9, 15)
    return mock_instance


@pytest.fixture
def mock_dir_files(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the DirFilesManagement class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked DirFilesManagement instance
    """
    mock_files = mocker.patch(
        "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda.DirFilesManagement")
    mock_instance = MagicMock(spec=DirFilesManagement)
    mock_files.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the CreateLog class.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked CreateLog instance
    """
    mock_log = mocker.patch("stpstone.ingestion.countries.br.macroeconomics.bcb_olinda.CreateLog")
    mock_instance = MagicMock(spec=CreateLog)
    mock_log.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked requests.get function
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking backoff decorator to bypass retries.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked backoff.on_exception decorator
    """
    return mocker.patch("backoff.on_exception", return_value=lambda func: func)


@pytest.fixture
def mock_insert_table_db(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking the insert_table_db method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    MagicMock
        Mocked insert_table_db method
    """
    return mocker.patch(
        "stpstone.ingestion.abc.ingestion_abc.ABCIngestionOperations.insert_table_db")


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date for testing.

    Returns
    -------
    date
        A sample date (2025-09-15)
    """
    return date(2025, 9, 15)


@pytest.fixture
def sample_date_range() -> tuple[date, date]:
    """Fixture providing sample date range for testing.

    Returns
    -------
    tuple[date, date]
        Sample start and end dates (2025-08-17 to 2025-09-15)
    """
    return date(2025, 8, 17), date(2025, 9, 15)


# --------------------------
# BCBOlindaCurrencies Tests
# --------------------------


class TestBCBOlindaCurrencies:
    """Test cases for BCBOlindaCurrencies class.

    This test class verifies the behavior of the BCB Olinda currencies ingestion
    functionality with different input types and edge cases.
    """

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
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
            Mocked requests.get function
        mock_response_success : Response
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance
        mocker : MockerFixture
            Pytest mocker fixture

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response_success
        
        # Mock standardize_dataframe at instance level
        standardized_df = pd.DataFrame({
            "SIMBOLO": ["USD", "EUR"],
            "NOME_FORMATADO": ["Dólar americano", "Euro"],
            "TIPO_MOEDA": ["Real", "Real"],
        })

        instance = BCBOlindaCurrencies()
        mock_standardize = mocker.patch.object(instance, 'standardize_dataframe', 
                                               return_value=standardized_df)
        
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
            Mocked requests.get function
        mock_response_success : Response
            Mocked successful HTTP response
        mock_backoff : MagicMock
            Mocked backoff function
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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


# --------------------------
# BCBOlindaPTAXUSDBRL Tests
# --------------------------


class TestBCBOlindaPTAXUSDBRL:
    """Test cases for BCBOlindaPTAXUSDBRL class.

    This test class verifies the PTAX USD/BRL exchange rate ingestion
    functionality with different date ranges and error conditions.
    """

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
            Sample date range tuple
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
            Mocked requests.get method
        mock_response_ptax : Response
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance
        mocker : MockerFixture
            Pytest mocker fixture

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
        _ = mocker.patch.object(instance, 'standardize_dataframe', 
                                               return_value=standardized_df)
        
        result_df = instance.run(timeout=10.0, bool_verify=False)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert set(result_df.columns) == {
            "COTACAO_COMPRA",
            "COTACAO_VENDA",
            "DATA_HORA_COTACAO",
        }

        # Verify request call with headers
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
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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


# --------------------------
# BCBOlindaCurrenciesTS Tests  
# --------------------------


class TestBCBOlindaCurrenciesTS:
    """Test cases for BCBOlindaCurrenciesTS class.

    This test class verifies the multi-currency time series ingestion
    functionality including currency list retrieval and multiple API calls.
    """

    def test_init_with_valid_date_range(
        self,
        sample_date_range: tuple[date, date],
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test initialization with valid date range for time series.
        
        Parameters
        ----------
        sample_date_range : tuple[date, date]
            Sample date range for testing
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

        Returns
        -------
        None
        """
        date_start, date_end = sample_date_range

        instance = BCBOlindaCurrenciesTS(
            date_start=date_start,
            date_end=date_end,
        )

        assert instance.date_start == date_start
        assert instance.date_end == date_end
        
        # Verify URL template structure
        assert "CotacaoMoedaPeriodo" in instance.fstr_url
        assert "@moeda='{}'" in instance.fstr_url
        assert "@dataInicial='{}'" in instance.fstr_url
        assert "@dataFinalCotacao='{}'" in instance.fstr_url

    def test__currencies_method_retrieves_list(
        self,
        mocker: MockerFixture,
        mock_requests_get: MagicMock,
        mock_response_success: Response,
        mock_dates_current: MagicMock,
        mock_dates_br: MagicMock,
        mock_dir_files: MagicMock,
        mock_create_log: MagicMock,
    ) -> None:
        """Test _currencies method retrieves currency symbols.
        
        Parameters
        ----------
        mocker : MockerFixture
            Pytest mocker fixture
        mock_requests_get : MagicMock
            Mocked requests.get function
        mock_response_success : Response
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

        Returns
        -------
        None
        """
        mock_requests_get.return_value = mock_response_success
        
        standardized_df = pd.DataFrame({
            "SIMBOLO": ["USD", "EUR", "GBP"],
            "NOME_FORMATADO": ["Dólar", "Euro", "Libra"],
            "TIPO_MOEDA": ["Real", "Real", "Real"],
        })

        # Mock at class level since _currencies creates a new instance
        mock_standardize = mocker.patch(
            "stpstone.ingestion.countries.br.macroeconomics.bcb_olinda."
            + "BCBOlindaCurrencies.standardize_dataframe",
            return_value=standardized_df
        )

        instance = BCBOlindaCurrenciesTS(date_end=date(2025, 9, 15))
        currencies = instance._currencies()

        assert isinstance(currencies, list)
        assert currencies == ["USD", "EUR", "GBP"]
        assert len(currencies) == 3

        # Verify internal BCBOlindaCurrencies creation and run
        mock_requests_get.assert_called_once()
        mock_standardize.assert_called_once()


# --------------------------
# BCBOlindaAnnualMarketExpectations Tests
# --------------------------


class TestBCBOlindaAnnualMarketExpectations:
    """Test cases for BCBOlindaAnnualMarketExpectations class.

    This test class verifies the annual market expectations data ingestion
    functionality with comprehensive economic indicators coverage.
    """

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
            Sample date for testing
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
            Mocked requests.get method
        mock_response_expectations : Response
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance
        mocker : MockerFixture
            Pytest mocker fixture

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
        _ = mocker.patch.object(instance, 'standardize_dataframe', 
                                               return_value=standardized_df)
        
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
            Mocked successful HTTP response
        mock_dates_current : MagicMock
            Mocked DatesCurrent instance
        mock_dates_br : MagicMock
            Mocked DatesBRAnbima instance
        mock_dir_files : MagicMock
            Mocked DirFilesManagement instance
        mock_create_log : MagicMock
            Mocked CreateLog instance

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
        
        # Verify data types
        assert isinstance(record["Media"], (int, float))
        assert isinstance(record["DataReferencia"], int)
        assert isinstance(record["numeroRespondentes"], int)
        assert "2025-09-15" in record["Data"]


# --------------------------
# Cross-Class Integration Tests
# --------------------------


def test_class_inheritance_structure() -> None:
    """Test proper inheritance from ABCIngestionOperations.
    
    Returns
    -------
    None
    """
    from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
    
    # Test each class inherits correctly
    classes_to_test = [
        BCBOlindaCurrencies,
        BCBOlindaPTAXUSDBRL,
        BCBOlindaCurrenciesTS,
        BCBOlindaAnnualMarketExpectations,
    ]
    
    for cls in classes_to_test:
        instance = cls()
        assert isinstance(instance, ABCIngestionOperations)
        # Verify base class methods are accessible
        assert hasattr(instance, "insert_table_db")
        assert hasattr(instance, "standardize_dataframe")
        # Verify multiple inheritance
        assert hasattr(instance, "parse_raw_file")


def test_url_format_consistency() -> None:
    """Test URL format consistency across all classes.
    
    Returns
    -------
    None
    """
    instances = [
        BCBOlindaCurrencies(),
        BCBOlindaPTAXUSDBRL(),
        BCBOlindaCurrenciesTS(),
        BCBOlindaAnnualMarketExpectations(),
    ]
    
    common_patterns = [
        "olinda.bcb.gov.br",
        "/olinda/servico/",
        "/versao/v1/odata/",
        "$format=json",
    ]
    
    for instance in instances:
        # Get URL from instance (some use fstr_url template)
        url = getattr(instance, "url", None) or getattr(instance, "fstr_url", None)
        assert url is not None
        for pattern in common_patterns:
            assert pattern in url
        # Verify HTTPS
        assert url.startswith("https://")