"""Unit tests for AnbimaDataIDAGeral module.

Tests the ingestion functionality for AnbimaDataIDAGeral including:
- Initialization with valid and invalid inputs
- Response retrieval and parsing
- Data transformation and database insertion
- Edge cases, error conditions, and type validation
"""

from datetime import date
import importlib
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response
from sqlalchemy.orm import Session

from stpstone.ingestion.countries.br.exchange.anbima_data_ida_geral import AnbimaDataIDAGeral
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
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mock object for requests.get
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample Excel content.

	Returns
	-------
	Response
		Mocked response object with Excel content
	"""
	response = MagicMock(spec=Response)
	response.url = "https://example.com/test.xls"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		A fixed date for consistent testing
	"""
	return date(2023, 1, 1)


@pytest.fixture
def mock_logger() -> MagicMock:
	"""Mock Logger object for testing.

	Returns
	-------
	MagicMock
		Mocked logger object
	"""
	return MagicMock()


@pytest.fixture
def mock_db_session() -> MagicMock:
	"""Mock database session for testing.

	Returns
	-------
	MagicMock
		Mocked SQLAlchemy session
	"""
	mock_session = MagicMock(spec=Session)
	mock_session.insert = MagicMock()
	return mock_session


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Provide a sample DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with expected IDAGeral structure (9 columns, no PMR)
	"""
	return pd.DataFrame(
		{
			"INDICE": ["TEST"],
			"DATA_REFERENCIA": [date(2023, 1, 1)],
			"NUMERO_INDICE": [100.0],
			"VARIACAO_DIARIA": [0.1],
			"VARIACAO_MES": [0.2],
			"VARIACAO_ANUAL": [0.3],
			"VARIACAO_12_MESES": [0.4],
			"VARIACAO_24_MESES": [0.5],
			"DURATION_DU": [1.0],
		}
	)


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> None:
	"""Mock backoff to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests for AnbimaDataIDAGeral
# --------------------------
class TestAnbimaDataIDAGeral:
	"""Test cases for AnbimaDataIDAGeral class."""

	@pytest.fixture
	def instance_no_db(self, sample_date: date, mock_logger: MagicMock) -> AnbimaDataIDAGeral:
		"""Fixture providing AnbimaDataIDAGeral instance without database.

		Parameters
		----------
		sample_date : date
			Sample date for initialization
		mock_logger : MagicMock
			Mocked logger

		Returns
		-------
		AnbimaDataIDAGeral
			Initialized instance without database
		"""
		return AnbimaDataIDAGeral(date_ref=sample_date, logger=mock_logger, cls_db=None)

	@pytest.fixture
	def instance(
		self,
		sample_date: date,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
	) -> AnbimaDataIDAGeral:
		"""Fixture providing AnbimaDataIDAGeral instance.

		Parameters
		----------
		sample_date : date
			Sample date for initialization
		mock_logger : MagicMock
			Mocked logger
		mock_db_session : MagicMock
			Mocked database session

		Returns
		-------
		AnbimaDataIDAGeral
			Initialized instance
		"""
		return AnbimaDataIDAGeral(date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session)

	def test_init_with_valid_inputs(self, instance: AnbimaDataIDAGeral, sample_date: date) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- Instance is properly initialized with provided date
		- Logger and database session are set
		- URL is correctly set
		- Inherited classes are properly initialized

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance
		sample_date : date
			Sample date used for initialization

		Returns
		-------
		None
		"""
		assert instance.date_ref == sample_date
		assert instance.url.endswith("IDAGERAL-HISTORICO.xls")
		assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
		assert isinstance(instance.cls_dates_current, DatesCurrent)
		assert isinstance(instance.cls_create_log, CreateLog)
		assert isinstance(instance.cls_dates_br, DatesBRAnbima)

	def test_init_without_date(self, mock_logger: MagicMock, mock_db_session: MagicMock) -> None:
		"""Test initialization without date_ref.

		Verifies
		--------
		- Instance uses default date (previous working day)
		- Other attributes are properly set

		Parameters
		----------
		mock_logger : MagicMock
			Mocked logger
		mock_db_session : MagicMock
			Mocked database session

		Returns
		-------
		None
		"""
		with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2023, 1, 1)):
			instance = AnbimaDataIDAGeral(logger=mock_logger, cls_db=mock_db_session)
		assert isinstance(instance.date_ref, date)
		assert instance.url.endswith("IDAGERAL-HISTORICO.xls")

	def test_get_response_success(
		self,
		instance: AnbimaDataIDAGeral,
		mock_requests_get: MagicMock,
		mock_response: Response,
	) -> None:
		"""Test successful response retrieval.

		Verifies
		--------
		- HTTP request is made with correct parameters
		- Response is returned as expected

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance
		mock_requests_get : MagicMock
			Mocked requests.get
		mock_response : Response
			Mocked response object

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		result = instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
		assert result == mock_response
		mock_requests_get.assert_called_once_with(instance.url, timeout=(12.0, 21.0), verify=True)
		mock_response.raise_for_status.assert_called_once()

	def test_parse_raw_file(
		self,
		instance: AnbimaDataIDAGeral,
		mock_response: Response,
	) -> None:
		"""Test parsing of raw file content.

		Verifies
		--------
		- Response URL is returned correctly

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance
		mock_response : Response
			Mocked response object

		Returns
		-------
		None
		"""
		result = instance.parse_raw_file(mock_response)
		assert result == mock_response.url
		assert isinstance(result, str)

	def test_transform_data(self, instance: AnbimaDataIDAGeral, mocker: MockerFixture) -> None:
		"""Test data transformation into DataFrame.

		Verifies
		--------
		- Excel file is read correctly
		- Column names are set as expected
		- Returns a pandas DataFrame

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_df = pd.DataFrame({"INDICE": ["TEST"]})
		mocker.patch("pandas.read_excel", return_value=mock_df)
		result = instance.transform_data("https://example.com/test.xls")
		pd.read_excel.assert_called_once_with(
			"https://example.com/test.xls",
			engine="openpyxl",
			names=[
				"INDICE",
				"DATA_REFERENCIA",
				"NUMERO_INDICE",
				"VARIACAO_DIARIA",
				"VARIACAO_MES",
				"VARIACAO_ANUAL",
				"VARIACAO_12_MESES",
				"VARIACAO_24_MESES",
				"DURATION_DU",
			],
		)
		assert isinstance(result, pd.DataFrame)
		assert result.equals(mock_df)

	def test_run_without_db(
		self,
		instance_no_db: AnbimaDataIDAGeral,
		mock_requests_get: MagicMock,
		mock_response: Response,
		sample_dataframe: pd.DataFrame,
		mocker: MockerFixture,
	) -> None:
		"""Test run method without database session.

		Verifies
		--------
		- Full ingestion process without DB insertion
		- Correct DataFrame is returned

		Parameters
		----------
		instance_no_db : AnbimaDataIDAGeral
			Initialized instance without database
		mock_requests_get : MagicMock
			Mocked requests.get
		mock_response : Response
			Mocked response object
		sample_dataframe : pd.DataFrame
			Sample DataFrame for testing
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mocker.patch.object(instance_no_db, "transform_data", return_value=sample_dataframe)
		mocker.patch.object(instance_no_db, "standardize_dataframe", return_value=sample_dataframe)
		result = instance_no_db.run()
		assert isinstance(result, pd.DataFrame)
		assert result.equals(sample_dataframe)
		mock_requests_get.assert_called_once()

	def test_run_with_db(
		self,
		instance: AnbimaDataIDAGeral,
		mock_requests_get: MagicMock,
		mock_response: Response,
		sample_dataframe: pd.DataFrame,
		mocker: MockerFixture,
	) -> None:
		"""Test run method with database session.

		Verifies
		--------
		- Full ingestion process with DB insertion
		- Database insertion method is called
		- No DataFrame is returned

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance
		mock_requests_get : MagicMock
			Mocked requests.get
		mock_response : Response
			Mocked response object
		sample_dataframe : pd.DataFrame
			Sample DataFrame for testing
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_requests_get.return_value = mock_response
		mocker.patch.object(instance, "transform_data", return_value=sample_dataframe)
		mocker.patch.object(instance, "standardize_dataframe", return_value=sample_dataframe)
		mocker.patch.object(instance, "insert_table_db")
		result = instance.run()
		assert result is None
		instance.insert_table_db.assert_called_once_with(
			cls_db=instance.cls_db,
			str_table_name="br_anbima_data_indexes_ida_geral",
			df_=sample_dataframe,
			bool_insert_or_ignore=False,
		)

	def test_run_invalid_timeout(self, instance: AnbimaDataIDAGeral) -> None:
		"""Test run method with invalid timeout.

		Verifies
		--------
		- TypeError is raised for invalid timeout types

		Parameters
		----------
		instance : AnbimaDataIDAGeral
			Initialized instance

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			instance.run(timeout="invalid")


# --------------------------
# Reload Tests
# --------------------------
def test_module_reload() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors
	- Class is properly reloaded

	Returns
	-------
	None
	"""
	importlib.reload(sys.modules["stpstone.ingestion.countries.br.exchange.anbima_data_ida_geral"])
	assert hasattr(
		sys.modules["stpstone.ingestion.countries.br.exchange.anbima_data_ida_geral"],
		"AnbimaDataIDAGeral",
	)
