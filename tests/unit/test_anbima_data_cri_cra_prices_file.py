"""Unit tests for AnbimaDataCRICRAPricesFile ingestion class."""

from datetime import date
from io import StringIO
from logging import Logger
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_file import (
	AnbimaDataCRICRAPricesFile,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
	"""Fixture providing mock logger.

	Returns
	-------
	Logger
		Mock logger instance.
	"""
	return Mock(spec=Logger)


@pytest.fixture
def mock_db_session() -> Session:
	"""Fixture providing mock database session.

	Returns
	-------
	Session
		Mock database session instance.
	"""
	return Mock(spec=Session)


@pytest.fixture
def sample_date_ref() -> date:
	"""Fixture providing sample reference date.

	Returns
	-------
	date
		Reference date for testing.
	"""
	return date(2024, 1, 15)


@pytest.fixture
def mock_response() -> Response:
	"""Fixture providing mock HTTP response.

	Returns
	-------
	Response
		Mock response instance.
	"""
	mock = Mock(spec=Response)
	mock.text = "test,data\n1,2\n3,4"
	mock.status_code = 200
	mock.raise_for_status = Mock()
	return mock


@pytest.fixture
def sample_csv_response() -> Response:
	"""Fixture providing mock HTTP response with realistic CRI/CRA CSV data.

	Returns
	-------
	Response
		Mock response with semicolon-delimited CSV content.
	"""
	csv_content = (
		"Data de Referência;Risco de Crédito;Emissor;Série;Emissão;Código;"
		"Vencimento;Índice de Correção;Taxa Compra;Taxa Venda;Taxa Indicativa;"
		"Desvio Padrão;PU;% PU Par % VNE;Duration;Data de Referência NTN-B;% Reune\n"
		"14/01/2024;AA;EMISSOR1;1;1;CRA001;14/01/2034;IPCA;5,5%;5,6%;5,55%;"
		"0,05%;1234,56;99,5%;3,5;-;-\n"
	)
	mock = Mock(spec=Response)
	mock.text = csv_content
	mock.status_code = 200
	mock.raise_for_status = Mock()
	return mock


# --------------------------
# Tests
# --------------------------
class TestAnbimaDataCRICRAPricesFile:
	"""Test cases for AnbimaDataCRICRAPricesFile class."""

	def test_init_with_valid_inputs(
		self, sample_date_ref: date, mock_logger: Logger, mock_db_session: Session
	) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- The class can be initialized with valid parameters.
		- All attributes are correctly set.
		- Download URL is properly configured.

		Parameters
		----------
		sample_date_ref : date
			Reference date fixture.
		mock_logger : Logger
			Mock logger fixture.
		mock_db_session : Session
			Mock database session fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(
			date_ref=sample_date_ref, logger=mock_logger, cls_db=mock_db_session
		)

		assert instance.date_ref == sample_date_ref
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert (
			instance.download_url
			== "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
		)

	def test_init_with_none_date_ref(self, mock_logger: Logger) -> None:
		"""Test initialization with None date_ref uses default date.

		Verifies
		--------
		- When date_ref is None, a default date is calculated.
		- The default date is not None.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(date_ref=None, logger=mock_logger)

		assert instance.date_ref is not None
		assert isinstance(instance.date_ref, date)

	@patch(
		"stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_file.requests.get"
	)
	def test_get_response_successful_download(
		self, mock_get: Mock, mock_logger: Logger, mock_response: Response
	) -> None:
		"""Test successful file download.

		Verifies
		--------
		- HTTP request is made with correct URL.
		- Response is returned successfully.
		- raise_for_status is called.

		Parameters
		----------
		mock_get : Mock
			Mock requests.get function.
		mock_logger : Logger
			Mock logger fixture.
		mock_response : Response
			Mock response fixture.

		Returns
		-------
		None
		"""
		mock_get.return_value = mock_response

		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.get_response()

		assert result == mock_response
		mock_get.assert_called_once()
		mock_response.raise_for_status.assert_called_once()

	@patch(
		"stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_file.requests.get"
	)
	def test_get_response_http_error(self, mock_get: Mock, mock_logger: Logger) -> None:
		"""Test get_response with HTTP error.

		Verifies
		--------
		- Exception is raised when HTTP error occurs.

		Parameters
		----------
		mock_get : Mock
			Mock requests.get function.
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		mock_get.side_effect = Exception("HTTP Error")

		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

		with pytest.raises(Exception, match="HTTP Error"):
			instance.get_response()

	@patch(
		"stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_file.requests.get"
	)
	def test_get_response_connection_error(self, mock_get: Mock, mock_logger: Logger) -> None:
		"""Test get_response raises on connection failure.

		Verifies
		--------
		- Connection errors are properly raised.

		Parameters
		----------
		mock_get : Mock
			Mock requests.get function.
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		mock_get.side_effect = ConnectionError("Connection failed")

		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

		with pytest.raises(ConnectionError, match="Connection failed"):
			instance.get_response()

	def test_parse_raw_file_with_valid_response(
		self, mock_logger: Logger, mock_response: Response
	) -> None:
		"""Test parse_raw_file with valid response.

		Verifies
		--------
		- StringIO is created from response text.
		- Content is correctly extracted.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.
		mock_response : Response
			Mock response fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.parse_raw_file(resp_req=mock_response)

		assert isinstance(result, StringIO)
		assert result.getvalue() == mock_response.text

	def test_transform_data_with_empty_response(
		self, mock_logger: Logger, mocker: MockerFixture
	) -> None:
		"""Test transform_data with empty CSV response raises.

		Verifies
		--------
		- Handles empty response by raising an exception.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.
		mocker : MockerFixture
			Pytest-mock fixture.

		Returns
		-------
		None
		"""
		mock_response = mocker.MagicMock(spec=Response)
		mock_response.text = ""

		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

		with pytest.raises(Exception):  # noqa B017: do not assert blind exception
			instance.transform_data(raw_data=mock_response)

	def test_transform_data_returns_dataframe(
		self, mock_logger: Logger, sample_csv_response: Response
	) -> None:
		"""Test transform_data returns a DataFrame from valid CSV.

		Verifies
		--------
		- Returns pd.DataFrame.
		- DataFrame has at least one row.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.
		sample_csv_response : Response
			Mock response with realistic CSV content.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.transform_data(raw_data=sample_csv_response)

		assert isinstance(result, pd.DataFrame)
		assert len(result) >= 1

	def test_transform_data_expected_columns(
		self, mock_logger: Logger, sample_csv_response: Response
	) -> None:
		"""Test transform_data produces expected column names.

		Verifies
		--------
		- Required columns are present in result.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.
		sample_csv_response : Response
			Mock response with realistic CSV content.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.transform_data(raw_data=sample_csv_response)

		expected_columns = [
			"DATA_REFERENCIA",
			"RISCO_CREDITO",
			"EMISSOR",
			"SERIE",  # codespell:ignore
			"EMISSAO",
			"CODIGO",
			"VENCIMENTO",
			"INDICE_CORRECAO",
			"TAXA_COMPRA",
			"TAXA_VENDA",
			"TAXA_INDICATIVA",
			"DESVIO_PADRAO",
			"PU",
			"PCT_PU_PAR_PCT_VNE",
			"DURATION",
			"DATA_REFERENCIA_NTNB",
			"PCT_REUNE",
		]

		for col in expected_columns:
			assert col in result.columns, f"Missing expected column: {col}"

	def test_transform_data_cleans_percentage_symbols(
		self, mock_logger: Logger, sample_csv_response: Response
	) -> None:
		"""Test transform_data removes percentage symbols from numeric columns.

		Verifies
		--------
		- Percentage signs are stripped from TAXA_COMPRA and similar fields.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.
		sample_csv_response : Response
			Mock response with CSV containing percentage symbols.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.transform_data(raw_data=sample_csv_response)

		for col in ["TAXA_COMPRA", "TAXA_VENDA", "TAXA_INDICATIVA"]:
			if col in result.columns and len(result) > 0:
				val = result[col].iloc[0]
				assert "%" not in str(val), f"Column {col} still contains '%': {val}"

	def test_transform_data_fills_missing_columns(self, mock_logger: Logger) -> None:
		"""Test transform_data adds missing expected columns as None.

		Verifies
		--------
		- When CSV is missing expected columns, they are added with None values.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		minimal_csv = "Emissor;Série\nEMISSOR1;1\n"
		mock_resp = Mock(spec=Response)
		mock_resp.text = minimal_csv
		mock_resp.raise_for_status = Mock()

		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)
		result = instance.transform_data(raw_data=mock_resp)

		assert isinstance(result, pd.DataFrame)
		assert "CODIGO" in result.columns

	def test_download_url_value(self, mock_logger: Logger) -> None:
		"""Test download URL is set to the expected ANBIMA endpoint.

		Verifies
		--------
		- download_url matches the known ANBIMA export endpoint.

		Parameters
		----------
		mock_logger : Logger
			Mock logger fixture.

		Returns
		-------
		None
		"""
		instance = AnbimaDataCRICRAPricesFile(logger=mock_logger)

		expected_url = "https://www.anbima.com.br/pt_br/anbima/TaxasCriCraExport/downloadExterno"
		assert instance.download_url == expected_url
