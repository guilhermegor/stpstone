"""Unit tests for FIFFactSheet ingestion module."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.fif_fact_sheet import FIFFactSheet


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
	"""Provide a mock logger.

	Returns
	-------
	MagicMock
		Mock logger instance.
	"""
	return MagicMock(spec=Logger)


@pytest.fixture
def mock_db_session() -> MagicMock:
	"""Provide a mock database session.

	Returns
	-------
	MagicMock
		Mock database session.
	"""
	return MagicMock(spec=Session)


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		Sample date (2023-12-01).
	"""
	return date(2023, 12, 1)


@pytest.fixture
def mock_zip_response() -> MagicMock:
	"""Provide a mock ZIP HTTP response.

	Returns
	-------
	MagicMock
		Mock response with ZIP content.
	"""
	response = MagicMock(spec=Response)

	zip_buffer = BytesIO()
	with zipfile.ZipFile(zip_buffer, "w") as zip_file:
		zip_file.writestr("lamina_fi_202312.csv", "CNPJ;DENOM\n12345678000195;Fund")

	response.content = zip_buffer.getvalue()
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_csv_content() -> str:
	"""Provide sample CSV content.

	Returns
	-------
	str
		Sample CSV data.
	"""
	return "CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC\n12345678000195;Test Fund;2023-12-01"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Provide sample fact sheet DataFrame.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame.
	"""
	return pd.DataFrame(
		{
			"CNPJ_FUNDO_CLASSE": ["12345678000195"],
			"DENOM_SOCIAL": ["Test Fund"],
		}
	)


# --------------------------
# Tests
# --------------------------
class TestFIFFactSheet:
	"""Test cases for FIFFactSheet class."""

	def test_init_url_construction(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test URL construction for fact sheet data.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.

		Returns
		-------
		None
		"""
		instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)

		expected_url = "https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/lamina_fi_202312.zip"
		assert instance.url == expected_url

	def test_get_response_success(
		self, mock_logger: MagicMock, sample_date: date, mock_zip_response: MagicMock
	) -> None:
		"""Test successful HTTP response retrieval.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		mock_zip_response : MagicMock
			Mock ZIP response.

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.ingestion.countries.br.registries.fif_fact_sheet.requests.get"
		) as mock_get:
			mock_get.return_value = mock_zip_response

			instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)
			result = instance.get_response()

			assert result == mock_zip_response

	def test_get_response_http_error(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test HTTP error propagates from get_response.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.ingestion.countries.br.registries.fif_fact_sheet.requests.get"
		) as mock_get:
			mock_response = MagicMock(spec=Response)
			mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
				"404 Not Found"
			)
			mock_get.return_value = mock_response

			instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(requests.exceptions.HTTPError):
				instance.get_response()

	def test_parse_raw_file_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test successful parsing extracts main lamina CSV.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		mock_zip_response : MagicMock
			Mock ZIP response.

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.ingestion.countries.br.registries.fif_fact_sheet.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"CNPJ;DENOM\n12345678000195;Fund"), "lamina_fi_202312.csv"),
				(BytesIO(b"CNPJ;TP\n12345678000195;A"), "lamina_fi_carteira_202312.csv"),
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)
			result_content, result_filename = instance.parse_raw_file(mock_zip_response)

			assert isinstance(result_content, StringIO)
			assert result_filename == "lamina_fi_202312.csv"

	def test_parse_raw_file_main_not_found_raises(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test ValueError raised when main fact sheet file not found.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		mock_zip_response : MagicMock
			Mock ZIP response.

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.ingestion.countries.br.registries.fif_fact_sheet.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"binary data"), "some_other_file.bin")
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(ValueError, match="not found"):
				instance.parse_raw_file(mock_zip_response)

	def test_transform_data_adds_file_name(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		sample_csv_content: str,
	) -> None:
		"""Test transform_data adds FILE_NAME column with year-month.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		sample_csv_content : str
			Sample CSV content.

		Returns
		-------
		None
		"""
		instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)
		result_df = instance.transform_data(StringIO(sample_csv_content))

		assert isinstance(result_df, pd.DataFrame)
		assert "FILE_NAME" in result_df.columns
		assert result_df["FILE_NAME"].iloc[0] == "lamina_fi_202312.csv"

	def test_run_without_db(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		sample_dataframe: pd.DataFrame,
	) -> None:
		"""Test run method without database session returns DataFrame.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		sample_dataframe : pd.DataFrame
			Sample DataFrame.

		Returns
		-------
		None
		"""
		with (
			patch.object(FIFFactSheet, "get_response") as mock_get_response,
			patch.object(FIFFactSheet, "parse_raw_file") as mock_parse,
			patch.object(FIFFactSheet, "transform_data") as mock_transform,
			patch.object(FIFFactSheet, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "test.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = FIFFactSheet(date_ref=sample_date, logger=mock_logger)
			result = instance.run()

			assert isinstance(result, pd.DataFrame)

	def test_run_with_db(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
		sample_date: date,
		sample_dataframe: pd.DataFrame,
	) -> None:
		"""Test run method with database session returns None.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		mock_db_session : MagicMock
			Mock database session.
		sample_date : date
			Sample date for testing.
		sample_dataframe : pd.DataFrame
			Sample DataFrame.

		Returns
		-------
		None
		"""
		with (
			patch.object(FIFFactSheet, "get_response") as mock_get_response,
			patch.object(FIFFactSheet, "parse_raw_file") as mock_parse,
			patch.object(FIFFactSheet, "transform_data") as mock_transform,
			patch.object(FIFFactSheet, "standardize_dataframe") as mock_standardize,
			patch.object(FIFFactSheet, "insert_table_db") as mock_insert_db,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "test.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = FIFFactSheet(
				date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
			)
			result = instance.run()

			mock_insert_db.assert_called_once_with(
				cls_db=mock_db_session,
				str_table_name="br_cvm_fif_fact_sheet",
				df_=sample_dataframe,
				bool_insert_or_ignore=False,
			)
			assert result is None
