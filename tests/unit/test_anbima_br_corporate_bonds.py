"""Unit tests for AnbimaExchangeInfosBRCorporateBonds ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.otc.anbima_br_corporate_bonds import (
	AnbimaExchangeInfosBRCorporateBonds,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
	"""Mock requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	MagicMock
		Mocked requests.get object.
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample content.

	Returns
	-------
	Response
		Mocked Response object with predefined content.
	"""
	response = MagicMock(spec=Response)
	response.content = b"Sample content"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		Fixed date for consistent testing.
	"""
	return date(2025, 9, 5)


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
	"""Mock Logger object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Logger
		Mocked Logger instance.
	"""
	return mocker.create_autospec(Logger)


@pytest.fixture
def mock_db_session(mocker: MockerFixture) -> Session:
	"""Mock database Session object with insert method.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Session
		Mocked database Session.
	"""
	mock_session = mocker.create_autospec(Session)
	mock_session.insert = MagicMock()
	return mock_session


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> None:
	"""Mock backoff decorator to eliminate retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def sample_corporate_bonds_data() -> StringIO:
	"""Provide sample corporate bonds data for parsing tests.

	Returns
	-------
	StringIO
		Sample data in the expected format (note: using decimal="," so 5,0 = 5.0).
	"""
	data = (
		"Header\n"
		"Header2\n"
		"Header3\n"
		"ABC123@Issuer@20251231@IPCA@5,0@5,1@5,05@0,2@4,8@5,2@1000,0@1,0@100,0@90,0@NTNB"
	)
	return StringIO(data)


# --------------------------
# Tests
# --------------------------
class TestAnbimaExchangeInfosBRCorporateBonds:
	"""Test cases for AnbimaExchangeInfosBRCorporateBonds class."""

	@pytest.fixture
	def corporate_bonds_instance(
		self,
		sample_date: date,
		mock_logger: Logger,
		mock_db_session: Session,
	) -> AnbimaExchangeInfosBRCorporateBonds:
		"""Create AnbimaExchangeInfosBRCorporateBonds instance.

		Parameters
		----------
		sample_date : date
			Sample date for initialization.
		mock_logger : Logger
			Mocked logger instance.
		mock_db_session : Session
			Mocked database session.

		Returns
		-------
		AnbimaExchangeInfosBRCorporateBonds
			Initialized instance.
		"""
		return AnbimaExchangeInfosBRCorporateBonds(
			date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
		)

	@pytest.fixture
	def corporate_bonds_instance_no_db(
		self,
		sample_date: date,
		mock_logger: Logger,
	) -> AnbimaExchangeInfosBRCorporateBonds:
		"""Create AnbimaExchangeInfosBRCorporateBonds instance without DB session.

		Parameters
		----------
		sample_date : date
			Sample date for initialization.
		mock_logger : Logger
			Mocked logger instance.

		Returns
		-------
		AnbimaExchangeInfosBRCorporateBonds
			Initialized instance without DB session.
		"""
		return AnbimaExchangeInfosBRCorporateBonds(
			date_ref=sample_date, logger=mock_logger, cls_db=None
		)

	def test_init_valid_inputs(
		self,
		sample_date: date,
		mock_logger: Logger,
		mock_db_session: Session,
	) -> None:
		"""Test initialization with valid inputs.

		Parameters
		----------
		sample_date : date
			Sample date for initialization.
		mock_logger : Logger
			Mocked logger instance.
		mock_db_session : Session
			Mocked database session.

		Returns
		-------
		None
		"""
		instance = AnbimaExchangeInfosBRCorporateBonds(
			date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
		)
		assert instance.date_ref == sample_date
		assert instance.date_ref_yymmdd == "250905"
		assert instance.url.endswith("db250905.txt")
		assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
		assert isinstance(instance.cls_dates_current, DatesCurrent)
		assert isinstance(instance.cls_create_log, CreateLog)
		assert isinstance(instance.cls_dates_br, DatesBRAnbima)

	def test_init_default_date(
		self,
		mocker: MockerFixture,
		mock_logger: Logger,
		mock_db_session: Session,
	) -> None:
		"""Test initialization with default date.

		Parameters
		----------
		mocker : MockerFixture
			Pytest-mock fixture.
		mock_logger : Logger
			Mocked logger instance.
		mock_db_session : Session
			Mocked database session.

		Returns
		-------
		None
		"""
		mock_dates_br = mocker.patch.object(
			DatesBRAnbima, "add_working_days", return_value=date(2025, 9, 5)
		)
		instance = AnbimaExchangeInfosBRCorporateBonds(
			logger=mock_logger, cls_db=mock_db_session
		)
		assert instance.date_ref == date(2025, 9, 5)
		assert instance.url.endswith("db250905.txt")
		mock_dates_br.assert_called_once()

	def test_get_response_success(
		self,
		corporate_bonds_instance: AnbimaExchangeInfosBRCorporateBonds,
		mock_requests_get: MagicMock,
		mock_response: Response,
		mock_backoff: None,
	) -> None:
		"""Test successful HTTP response.

		Parameters
		----------
		corporate_bonds_instance : AnbimaExchangeInfosBRCorporateBonds
			Test instance.
		mock_requests_get : MagicMock
			Mocked requests.get.
		mock_response : Response
			Mocked response object.
		mock_backoff : None
			Mocked backoff decorator.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		result = corporate_bonds_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
		assert result == mock_response
		mock_requests_get.assert_called_once_with(
			corporate_bonds_instance.url, timeout=(12.0, 21.0), verify=True
		)
		mock_response.raise_for_status.assert_called_once()

	def test_parse_raw_file(
		self,
		corporate_bonds_instance: AnbimaExchangeInfosBRCorporateBonds,
		mock_response: Response,
		mocker: MockerFixture,
	) -> None:
		"""Test parsing of raw file content.

		Parameters
		----------
		corporate_bonds_instance : AnbimaExchangeInfosBRCorporateBonds
			Test instance.
		mock_response : Response
			Mocked response object.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_get_file = mocker.patch.object(
			corporate_bonds_instance, "get_file", return_value=StringIO("test content")
		)
		result = corporate_bonds_instance.parse_raw_file(mock_response)
		assert isinstance(result, StringIO)
		mock_get_file.assert_called_once_with(resp_req=mock_response)

	def test_transform_data(
		self,
		corporate_bonds_instance: AnbimaExchangeInfosBRCorporateBonds,
		sample_corporate_bonds_data: StringIO,
	) -> None:
		"""Test data transformation into DataFrame.

		Parameters
		----------
		corporate_bonds_instance : AnbimaExchangeInfosBRCorporateBonds
			Test instance.
		sample_corporate_bonds_data : StringIO
			Sample data for testing.

		Returns
		-------
		None
		"""
		df_ = corporate_bonds_instance.transform_data(sample_corporate_bonds_data)
		assert isinstance(df_, pd.DataFrame)
		assert list(df_.columns) == [
			"CODIGO", "NOME_EMISSOR", "DT_REPACTUACAO_VENCIMENTO", "INDICE_CORRECAO",
			"TX_COMPRA", "TX_VENDA", "TX_INDICATIVA", "DESVIO_PADRAO",
			"INTERVALO_INDICATIVO_MIN", "INTERVALO_INDICATIVO_MAX", "PU",
			"RATIO_PU_PAR_VNE", "DURATION", "PCT_REUNE", "REF_NTNB",
		]
		assert df_["TX_COMPRA"].iloc[0] == pytest.approx(5.0)
		assert df_["CODIGO"].iloc[0] == "ABC123"

	def test_run_without_db(
		self,
		corporate_bonds_instance_no_db: AnbimaExchangeInfosBRCorporateBonds,
		mock_requests_get: MagicMock,
		mock_response: Response,
		sample_corporate_bonds_data: StringIO,
		mocker: MockerFixture,
		mock_backoff: None,
	) -> None:
		"""Test full run without database session.

		Parameters
		----------
		corporate_bonds_instance_no_db : AnbimaExchangeInfosBRCorporateBonds
			Test instance without DB session.
		mock_requests_get : MagicMock
			Mocked requests.get.
		mock_response : Response
			Mocked response object.
		sample_corporate_bonds_data : StringIO
			Sample data for testing.
		mocker : MockerFixture
			Pytest-mock fixture.
		mock_backoff : None
			Mocked backoff decorator.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mocker.patch.object(
			corporate_bonds_instance_no_db, "get_file", return_value=sample_corporate_bonds_data
		)
		mocker.patch.object(
			corporate_bonds_instance_no_db,
			"standardize_dataframe",
			return_value=pd.DataFrame(),
		)
		result = corporate_bonds_instance_no_db.run()
		assert isinstance(result, pd.DataFrame)
		mock_requests_get.assert_called_once()
		corporate_bonds_instance_no_db.standardize_dataframe.assert_called_once()

	def test_run_with_db(
		self,
		corporate_bonds_instance: AnbimaExchangeInfosBRCorporateBonds,
		mock_requests_get: MagicMock,
		mock_response: Response,
		sample_corporate_bonds_data: StringIO,
		mocker: MockerFixture,
		mock_backoff: None,
	) -> None:
		"""Test full run with database session.

		Parameters
		----------
		corporate_bonds_instance : AnbimaExchangeInfosBRCorporateBonds
			Test instance.
		mock_requests_get : MagicMock
			Mocked requests.get.
		mock_response : Response
			Mocked response object.
		sample_corporate_bonds_data : StringIO
			Sample data for testing.
		mocker : MockerFixture
			Pytest-mock fixture.
		mock_backoff : None
			Mocked backoff decorator.

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mocker.patch.object(
			corporate_bonds_instance, "get_file", return_value=sample_corporate_bonds_data
		)
		mocker.patch.object(
			corporate_bonds_instance, "standardize_dataframe", return_value=pd.DataFrame()
		)
		mock_insert = mocker.patch.object(corporate_bonds_instance, "insert_table_db")
		result = corporate_bonds_instance.run()
		assert result is None
		mock_insert.assert_called_once()
