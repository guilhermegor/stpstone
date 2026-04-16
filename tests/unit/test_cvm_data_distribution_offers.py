"""Unit tests for CVMDataDistributionOffers ingestion module."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.cvm_data_distribution_offers import (
	CVMDataDistributionOffers,
)


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
		zip_file.writestr(
			"oferta_distribuicao.csv",
			"numero_processo;tipo_oferta\n001/2023;CRA",
		)

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
	return "numero_processo;tipo_oferta;valor_total\n001/2023;CRA;1000000"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Provide sample distribution offers DataFrame.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame.
	"""
	return pd.DataFrame(
		{
			"NUMERO_PROCESSO": ["001/2023"],
			"TIPO_OFERTA": ["CRA"],
			"VALOR_TOTAL": ["1000000"],
		}
	)


# --------------------------
# Tests
# --------------------------
class TestCVMDataDistributionOffers:
	"""Test cases for CVMDataDistributionOffers class."""

	def test_init_static_url(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test URL is the static oferta_distribuicao.zip endpoint.

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
		instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)

		assert (
			instance.url
			== "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"
		)

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
			"stpstone.ingestion.countries.br.registries.cvm_data_distribution_offers.requests.get"
		) as mock_get:
			mock_get.return_value = mock_zip_response

			instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)
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
		with patch(
			"stpstone.ingestion.countries.br.registries.cvm_data_distribution_offers.requests.get"
		) as mock_get:
			mock_response = MagicMock(spec=Response)
			mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
				"404 Not Found"
			)
			mock_get.return_value = mock_response

			instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(requests.exceptions.HTTPError):
				instance.get_response()

	def test_parse_raw_file_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test successful parsing extracts oferta_distribuicao.csv.

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
			"stpstone.ingestion.countries.br.registries"
			".cvm_data_distribution_offers.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(
					BytesIO(b"NUMERO_PROCESSO;TIPO_OFERTA\n001/2023;CRA"),
					"oferta_distribuicao.csv",
				)
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)
			result_content, result_filename = instance.parse_raw_file(mock_zip_response)

			assert isinstance(result_content, StringIO)
			assert result_filename == "oferta_distribuicao.csv"

	def test_parse_raw_file_no_csv_raises(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test ValueError raised when no CSV found in ZIP.

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
			"stpstone.ingestion.countries.br.registries"
			".cvm_data_distribution_offers.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"binary data"), "file.bin")
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(ValueError, match="not found"):
				instance.parse_raw_file(mock_zip_response)

	def test_transform_data_uppercases_columns(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		sample_csv_content: str,
	) -> None:
		"""Test transform_data uppercases column names.

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
		instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)
		result_df = instance.transform_data(StringIO(sample_csv_content))

		assert isinstance(result_df, pd.DataFrame)
		for col in result_df.columns:
			assert col == col.upper()

	def test_transform_data_adds_file_name(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		sample_csv_content: str,
	) -> None:
		"""Test transform_data adds FILE_NAME column.

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
		instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)
		result_df = instance.transform_data(StringIO(sample_csv_content))

		assert "FILE_NAME" in result_df.columns
		assert result_df["FILE_NAME"].iloc[0] == "oferta_distribuicao.csv"

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
			patch.object(CVMDataDistributionOffers, "get_response") as mock_get_response,
			patch.object(CVMDataDistributionOffers, "parse_raw_file") as mock_parse,
			patch.object(CVMDataDistributionOffers, "transform_data") as mock_transform,
			patch.object(CVMDataDistributionOffers, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "oferta_distribuicao.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = CVMDataDistributionOffers(date_ref=sample_date, logger=mock_logger)
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
			patch.object(CVMDataDistributionOffers, "get_response") as mock_get_response,
			patch.object(CVMDataDistributionOffers, "parse_raw_file") as mock_parse,
			patch.object(CVMDataDistributionOffers, "transform_data") as mock_transform,
			patch.object(CVMDataDistributionOffers, "standardize_dataframe") as mock_standardize,
			patch.object(CVMDataDistributionOffers, "insert_table_db") as mock_insert_db,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "oferta_distribuicao.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = CVMDataDistributionOffers(
				date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
			)
			result = instance.run()

			mock_insert_db.assert_called_once_with(
				cls_db=mock_db_session,
				str_table_name="br_cvm_distribution_offers",
				df_=sample_dataframe,
				bool_insert_or_ignore=False,
			)
			assert result is None
