"""Unit tests for CvmFIFCDA ingestion module."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.cvm_fif_cda import CvmFIFCDA


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
		zip_file.writestr("cda_file1.csv", "CNPJ;TP_ATIVO\n12345678000195;ACAO")

	response.content = zip_buffer.getvalue()
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


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
def sample_dataframe() -> pd.DataFrame:
	"""Provide sample CDA DataFrame.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame.
	"""
	return pd.DataFrame(
		{
			"CNPJ": ["12345678000195"],
			"TP_ATIVO": ["ACAO"],
			"FILE_NAME": ["cda_fi_202312_BLC_1.csv"],
		}
	)


# --------------------------
# Tests
# --------------------------
class TestFIFCDA:
	"""Test cases for CvmFIFCDA class."""

	def test_init_url_construction(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test URL construction for CDA data.

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
		instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)

		expected_url = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_202312.zip"
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
		patch_path = "stpstone.ingestion.countries.br.registries.cvm_fif_cda.requests.get"
		with patch(patch_path) as mock_get:
			mock_get.return_value = mock_zip_response

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)
			result = instance.get_response()

			assert result == mock_zip_response
			mock_zip_response.raise_for_status.assert_called_once()

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
		patch_path = "stpstone.ingestion.countries.br.registries.cvm_fif_cda.requests.get"
		with patch(patch_path) as mock_get:
			mock_response = MagicMock(spec=Response)
			mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
				"404 Not Found"
			)
			mock_get.return_value = mock_response

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(requests.exceptions.HTTPError):
				instance.get_response()

	def test_parse_raw_file_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test successful parsing of ZIP with multiple CSVs.

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
			"stpstone.ingestion.countries.br.registries.cvm_fif_cda.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"CNPJ;TP_ATIVO\n12345678000195;ACAO"), "file1.csv"),
				(BytesIO(b"CNPJ;TP_ATIVO\n98765432000198;RENDA_FIXA"), "file2.csv"),
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)
			result = instance.parse_raw_file(mock_zip_response)

			assert isinstance(result, list)
			assert len(result) == 2
			for content, filename in result:
				assert isinstance(content, StringIO)
				assert filename.endswith(".csv")

	def test_parse_raw_file_no_csv_found(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test ValueError raised when ZIP has no CSV files.

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
			"stpstone.ingestion.countries.br.registries.cvm_fif_cda.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"binary data"), "file.bin")
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(ValueError, match="No CSV files found"):
				instance.parse_raw_file(mock_zip_response)

	def test_transform_data_multiple_files(
		self, mock_logger: MagicMock, sample_date: date
	) -> None:
		"""Test transformation consolidates multiple CSV files.

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
		files_list = [
			(StringIO("CNPJ;TP_ATIVO\n12345678000195;ACAO"), "file1.csv"),
			(StringIO("CNPJ;TP_ATIVO\n98765432000198;FUNDO"), "file2.csv"),
		]

		instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)
		result_df = instance.transform_data(files_list=files_list)

		assert isinstance(result_df, pd.DataFrame)
		assert len(result_df) == 2
		assert "FILE_NAME" in result_df.columns
		assert set(result_df["FILE_NAME"].unique()) == {"file1.csv", "file2.csv"}

	def test_transform_data_empty_file_skipped(
		self, mock_logger: MagicMock, sample_date: date
	) -> None:
		"""Test empty files are skipped during transformation.

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
		files_list = [
			(StringIO(""), "empty_file.csv"),
			(StringIO("CNPJ;TP_ATIVO\n12345678000195;ACAO"), "valid_file.csv"),
		]

		instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)
		result_df = instance.transform_data(files_list=files_list)

		assert len(result_df) == 1
		assert result_df["FILE_NAME"].iloc[0] == "valid_file.csv"

	def test_transform_data_no_valid_files_raises(
		self, mock_logger: MagicMock, sample_date: date
	) -> None:
		"""Test ValueError raised when no valid data can be loaded.

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
		instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)

		with pytest.raises(ValueError, match="No valid data could be loaded"):
			instance.transform_data(files_list=[(StringIO(""), "empty.csv")])

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
		files_list = [(StringIO("CNPJ;TP\n1;A"), "f.csv")]

		with (
			patch.object(CvmFIFCDA, "get_response") as mock_get_response,
			patch.object(CvmFIFCDA, "parse_raw_file") as mock_parse,
			patch.object(CvmFIFCDA, "transform_data") as mock_transform,
			patch.object(CvmFIFCDA, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = files_list
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger)
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
		files_list = [(StringIO("CNPJ;TP\n1;A"), "f.csv")]

		with (
			patch.object(CvmFIFCDA, "get_response") as mock_get_response,
			patch.object(CvmFIFCDA, "parse_raw_file") as mock_parse,
			patch.object(CvmFIFCDA, "transform_data") as mock_transform,
			patch.object(CvmFIFCDA, "standardize_dataframe") as mock_standardize,
			patch.object(CvmFIFCDA, "insert_table_db") as mock_insert_db,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = files_list
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = CvmFIFCDA(date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session)
			result = instance.run()

			mock_insert_db.assert_called_once_with(
				cls_db=mock_db_session,
				str_table_name="br_cvm_fif_cda",
				df_=sample_dataframe,
				bool_insert_or_ignore=False,
			)
			assert result is None
