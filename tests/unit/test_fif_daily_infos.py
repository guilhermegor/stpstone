"""Unit tests for FIFDailyInfos ingestion module."""

from datetime import date
from io import BytesIO, StringIO
from logging import Logger
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
import pytest
import requests
from requests import Response, Session

from stpstone.ingestion.countries.br.registries.fif_daily_infos import FIFDailyInfos


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
def mock_response() -> MagicMock:
	"""Provide a mock HTTP response.

	Returns
	-------
	MagicMock
		Mock response object with sample content.
	"""
	response = MagicMock(spec=Response)
	response.content = b"Sample content"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_zip_response() -> MagicMock:
	"""Provide a mock ZIP HTTP response.

	Returns
	-------
	MagicMock
		Mock response object with ZIP content.
	"""
	response = MagicMock(spec=Response)

	zip_buffer = BytesIO()
	with zipfile.ZipFile(zip_buffer, "w") as zip_file:
		zip_file.writestr("test_file.csv", "CNPJ;VALOR\n12345678000195;1000.0")

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
def sample_csv_content() -> str:
	"""Provide sample CSV content.

	Returns
	-------
	str
		Sample CSV data.
	"""
	return "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA\n12345678000195;2023-12-01;1000000.0;1.2345"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Provide sample FIF daily data DataFrame.

	Returns
	-------
	pd.DataFrame
		Sample DataFrame with FIF daily data structure.
	"""
	return pd.DataFrame(
		{
			"CNPJ_FUNDO": ["12345678000195"],
			"DT_COMPTC": ["2023-12-01"],
			"VL_TOTAL": [1000000.0],
			"VL_QUOTA": [1.2345],
			"VL_PATRIM_LIQ": [950000.0],
			"CAPTC_DIA": [50000.0],
			"RESG_DIA": [25000.0],
			"NR_COTST": [150],
		}
	)


# --------------------------
# Tests
# --------------------------
class TestFIFDailyInfos:
	"""Test cases for FIFDailyInfos class."""

	def test_init_with_default_date(self, mock_logger: MagicMock) -> None:
		"""Test initialization with default date.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.

		Returns
		-------
		None
		"""
		with (
			patch(
				"stpstone.ingestion.countries.br.registries.fif_daily_infos.DatesCurrent"
			) as mock_dates_current,
			patch(
				"stpstone.ingestion.countries.br.registries.fif_daily_infos.DatesBRAnbima"
			) as mock_dates_br,
		):
			mock_dates_current_instance = MagicMock()
			mock_dates_current_instance.curr_date.return_value = date(2023, 12, 15)
			mock_dates_current.return_value = mock_dates_current_instance

			mock_dates_br_instance = MagicMock()
			mock_dates_br_instance.add_working_days.return_value = date(2023, 12, 11)
			mock_dates_br.return_value = mock_dates_br_instance

			instance = FIFDailyInfos(logger=mock_logger)

			assert instance.date_ref == date(2023, 12, 11)
			assert "inf_diario_fi_202312.zip" in instance.url
			assert instance.logger == mock_logger

	def test_init_with_custom_date(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test initialization with custom date reference.

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
		instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

		assert instance.date_ref == sample_date
		assert "inf_diario_fi_202312.zip" in instance.url

	def test_get_response_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_response: MagicMock,
	) -> None:
		"""Test successful HTTP response retrieval.

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance.
		sample_date : date
			Sample date for testing.
		mock_response : MagicMock
			Mock HTTP response.

		Returns
		-------
		None
		"""
		with patch(
			"stpstone.ingestion.countries.br.registries.fif_daily_infos.requests.get"
		) as mock_get:
			mock_get.return_value = mock_response

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)
			result = instance.get_response(timeout=(10, 20), bool_verify=True)

			mock_get.assert_called_once_with(
				instance.url,
				timeout=(10, 20),
				verify=True,
			)
			mock_response.raise_for_status.assert_called_once()
			assert result == mock_response

	def test_get_response_http_error(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test HTTP error handling during response retrieval.

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
			"stpstone.ingestion.countries.br.registries.fif_daily_infos.requests.get"
		) as mock_get:
			mock_response = MagicMock(spec=Response)
			mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
				"404 Not Found"
			)
			mock_get.return_value = mock_response

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(requests.exceptions.HTTPError):
				instance.get_response()

	def test_parse_raw_file_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test successful parsing of ZIP file content.

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
			"stpstone.ingestion.countries.br.registries.fif_daily_infos.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"CNPJ;VALOR\n12345678000195;1000.0"), "test_file.csv")
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)
			result_content, result_filename = instance.parse_raw_file(mock_zip_response)

			assert isinstance(result_content, StringIO)
			assert result_filename == "test_file.csv"

	def test_parse_raw_file_no_csv_found(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test error raised when no CSV files exist in ZIP.

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
			"stpstone.ingestion.countries.br.registries.fif_daily_infos.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = [
				(BytesIO(b"binary data"), "test_file.txt")
			]
			mock_dir_files.return_value = mock_dir_files_instance

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(ValueError, match="No CSV file found"):
				instance.parse_raw_file(mock_zip_response)

	def test_parse_raw_file_empty_zip(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		mock_zip_response: MagicMock,
	) -> None:
		"""Test error raised when ZIP archive is empty.

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
			"stpstone.ingestion.countries.br.registries.fif_daily_infos.DirFilesManagement"
		) as mock_dir_files:
			mock_dir_files_instance = MagicMock()
			mock_dir_files_instance.recursive_unzip_in_memory.return_value = []
			mock_dir_files.return_value = mock_dir_files_instance

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

			with pytest.raises(ValueError, match="No files found"):
				instance.parse_raw_file(mock_zip_response)

	def test_transform_data_success(
		self,
		mock_logger: MagicMock,
		sample_date: date,
		sample_csv_content: str,
	) -> None:
		"""Test successful transformation of CSV data.

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
		csv_io = StringIO(sample_csv_content)
		instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

		result_df = instance.transform_data(csv_io)

		assert isinstance(result_df, pd.DataFrame)
		assert "FILE_NAME" in result_df.columns
		assert result_df["FILE_NAME"].iloc[0] == "inf_diario_fi_202312.csv"
		assert len(result_df) == 1

	def test_transform_data_empty_input(self, mock_logger: MagicMock, sample_date: date) -> None:
		"""Test transform_data raises on empty CSV input.

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
		import pandas as pd

		instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)

		with pytest.raises(pd.errors.EmptyDataError):
			instance.transform_data(StringIO(""))

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
			patch.object(FIFDailyInfos, "get_response") as mock_get_response,
			patch.object(FIFDailyInfos, "parse_raw_file") as mock_parse,
			patch.object(FIFDailyInfos, "transform_data") as mock_transform,
			patch.object(FIFDailyInfos, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "test.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = FIFDailyInfos(date_ref=sample_date, logger=mock_logger)
			result = instance.run()

			mock_get_response.assert_called_once()
			mock_parse.assert_called_once()
			mock_transform.assert_called_once()
			mock_standardize.assert_called_once()

			assert isinstance(result, pd.DataFrame)
			assert result.equals(sample_dataframe)

	def test_run_with_db(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
		sample_date: date,
		sample_dataframe: pd.DataFrame,
	) -> None:
		"""Test run method with database session inserts data and returns None.

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
			patch.object(FIFDailyInfos, "get_response") as mock_get_response,
			patch.object(FIFDailyInfos, "parse_raw_file") as mock_parse,
			patch.object(FIFDailyInfos, "transform_data") as mock_transform,
			patch.object(FIFDailyInfos, "standardize_dataframe") as mock_standardize,
			patch.object(FIFDailyInfos, "insert_table_db") as mock_insert_db,
		):
			mock_get_response.return_value = MagicMock()
			mock_parse.return_value = (StringIO(), "test.csv")
			mock_transform.return_value = sample_dataframe
			mock_standardize.return_value = sample_dataframe

			instance = FIFDailyInfos(
				date_ref=sample_date, logger=mock_logger, cls_db=mock_db_session
			)
			result = instance.run()

			mock_insert_db.assert_called_once_with(
				cls_db=mock_db_session,
				str_table_name="br_cvm_data",
				df_=sample_dataframe,
				bool_insert_or_ignore=False,
			)
			assert result is None
