"""Unit tests for IRSBRShareholders class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling
- Data parsing and transformation
- Database insertion and fallback logic
"""

from datetime import date
from io import BytesIO
from typing import Union
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.taxation.irsbr_shareholders import (
	_COLUMN_NAMES,
	_NUM_FILES,
	IRSBRShareholders,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Helpers
# --------------------------
def _build_zip_bytes(csv_content: str, csv_filename: str = "Socios0.csv") -> BytesIO:
	"""Build an in-memory ZIP archive containing a single CSV file.

	Parameters
	----------
	csv_content : str
		The raw CSV text to embed inside the archive.
	csv_filename : str, optional
		The name of the CSV entry, by default "Socios0.csv".

	Returns
	-------
	BytesIO
		A seeked-to-zero bytes buffer of the ZIP archive.
	"""
	buf = BytesIO()
	with ZipFile(buf, "w") as zf:
		zf.writestr(csv_filename, csv_content)
	buf.seek(0)
	return buf


def _sample_csv_row() -> str:
	"""Return a single semicolon-separated CSV row matching _COLUMN_NAMES.

	Returns
	-------
	str
		One complete data row with 11 fields.
	"""
	return "12345678;2;JOAO DA SILVA;***000000**;22;20200115;;***111111**;MARIA SOUZA;22;4"


def _sample_csv_rows(n: int = 3) -> str:
	"""Return *n* semicolon-separated CSV rows.

	Parameters
	----------
	n : int, optional
		Number of rows to generate, by default 3.

	Returns
	-------
	str
		Multiple newline-separated data rows.
	"""
	return "\n".join([_sample_csv_row() for _ in range(n)])


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		Fixed date for consistent testing.
	"""
	return date(2025, 3, 15)


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
	"""Mock backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Mocked backoff.on_exception object.
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
	"""Mock requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Mocked requests.get object.
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample ZIP content.

	Returns
	-------
	Response
		Mocked Response object with predefined ZIP content.
	"""
	zip_bytes = _build_zip_bytes(_sample_csv_rows())
	response = MagicMock(spec=Response)
	response.content = zip_bytes.read()
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def irsbr_instance(sample_date: date) -> IRSBRShareholders:
	"""Fixture providing IRSBRShareholders instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	IRSBRShareholders
		Initialized IRSBRShareholders instance.
	"""
	return IRSBRShareholders(date_ref=sample_date)


@pytest.fixture
def sample_zip_file() -> BytesIO:
	"""Provide a sample ZIP file containing a CSV for parsing.

	Returns
	-------
	BytesIO
		In-memory ZIP archive with sample CSV data.
	"""
	return _build_zip_bytes(_sample_csv_rows())


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- The IRSBRShareholders instance is initialized correctly.
	- Attributes are set as expected.
	- Year-month formatting works correctly.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	instance = IRSBRShareholders(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert instance.year_month == "2025-03"
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_with_default_date() -> None:
	"""Test initialization with default date.

	Verifies
	--------
	- Default date is set to previous working day.
	- URL directory is correctly formatted.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 3, 14)):
		instance = IRSBRShareholders()
		assert instance.date_ref == date(2025, 3, 14)
		assert instance.year_month == "2025-03"
		assert "2025-03" in instance.dir_url


def test_init_url_format(sample_date: date) -> None:
	"""Test that the directory URL is built correctly from date_ref.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	instance = IRSBRShareholders(date_ref=sample_date)
	assert instance.dir_url.startswith(
		"https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9"
	)
	assert "?dir=/2025-03" in instance.dir_url


def test_get_response_success(
	irsbr_instance: IRSBRShareholders,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	--------
	- Successful HTTP request returns Response object.
	- Correct parameters are passed to requests.get.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_requests_get : object
		Mocked requests.get function.
	mock_response : Response
		Mocked Response object.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = irsbr_instance.get_response(
		timeout=(12.0, 21.0), bool_verify=False, url="https://example.com/test.zip"
	)
	assert isinstance(result, Response)
	mock_response.raise_for_status.assert_called_once()


@pytest.mark.parametrize(
	"timeout",
	[
		10,
		10.5,
		(5.0, 10.0),
		(5, 10),
	],
)
def test_get_response_timeout_variations(
	irsbr_instance: IRSBRShareholders,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response with various timeout inputs.

	Verifies
	--------
	- Different timeout formats are handled correctly.
	- Requests are made with correct timeout parameters.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_requests_get : object
		Mocked requests.get function.
	mock_response : Response
		Mocked Response object.
	mock_backoff : object
		Mocked backoff decorator.
	timeout : Union[int, float, tuple]
		Timeout value or tuple to test.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = irsbr_instance.get_response(timeout=timeout)
	assert isinstance(result, Response)


def test_parse_raw_file(
	irsbr_instance: IRSBRShareholders,
	mock_response: Response,
) -> None:
	"""Test parsing of raw file content.

	Verifies
	--------
	- Response content is correctly parsed into BytesIO.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_response : Response
		Mocked Response object.

	Returns
	-------
	None
	"""
	result = irsbr_instance.parse_raw_file(mock_response)
	assert isinstance(result, BytesIO)


def test_transform_data(
	irsbr_instance: IRSBRShareholders,
	sample_zip_file: BytesIO,
) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	--------
	- Input ZIP file is correctly transformed into DataFrame.
	- Column names match the expected schema.
	- Data is read correctly.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	sample_zip_file : BytesIO
		Sample ZIP file content.

	Returns
	-------
	None
	"""
	df_ = irsbr_instance.transform_data(file=sample_zip_file)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty, "DataFrame should not be empty"
	assert list(df_.columns) == _COLUMN_NAMES
	assert len(df_) == 3
	assert df_["EIN_BASIC"].iloc[0] == "12345678"
	assert df_["NAME"].iloc[0] == "JOAO DA SILVA"


def test_transform_data_none_input(irsbr_instance: IRSBRShareholders) -> None:
	"""Test transform_data with None input.

	Verifies
	--------
	- None input results in empty DataFrame.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.

	Returns
	-------
	None
	"""
	df_ = irsbr_instance.transform_data(file=None)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_transform_data_invalid_zip(irsbr_instance: IRSBRShareholders) -> None:
	"""Test transform_data with invalid ZIP content.

	Verifies
	--------
	- Invalid ZIP content results in empty DataFrame.
	- No errors are raised.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.

	Returns
	-------
	None
	"""
	invalid_zip = BytesIO(b"this is not a zip file")
	df_ = irsbr_instance.transform_data(file=invalid_zip)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_transform_data_zip_without_csv(irsbr_instance: IRSBRShareholders) -> None:
	"""Test transform_data with ZIP containing no CSV files.

	Verifies
	--------
	- ZIP without CSV entries results in empty DataFrame.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.

	Returns
	-------
	None
	"""
	buf = BytesIO()
	with ZipFile(buf, "w") as zf:
		zf.writestr("readme.txt", "no csv here")
	buf.seek(0)
	df_ = irsbr_instance.transform_data(file=buf)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db(
	irsbr_instance: IRSBRShareholders,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Full ingestion pipeline works without database.
	- Returns transformed DataFrame.
	- All intermediate methods are called correctly.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_requests_get : object
		Mocked requests.get function.
	mock_response : Response
		Mocked Response object.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	sample_df = pd.DataFrame({"EIN_BASIC": ["12345678"]})
	with (
		patch.object(irsbr_instance, "parse_raw_file", return_value=BytesIO(b"")),
		patch.object(irsbr_instance, "transform_data", return_value=sample_df),
		patch.object(
			irsbr_instance, "standardize_dataframe", return_value=sample_df
		) as mock_standardize,
	):
		result = irsbr_instance.run()
		assert isinstance(result, pd.DataFrame)
		mock_standardize.assert_called_once()


def test_run_with_db(
	irsbr_instance: IRSBRShareholders,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Database insertion is called when cls_db is provided.
	- No DataFrame is returned.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_requests_get : object
		Mocked requests.get function.
	mock_response : Response
		Mocked Response object.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	irsbr_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	sample_df = pd.DataFrame({"EIN_BASIC": ["12345678"]})
	with (
		patch.object(irsbr_instance, "parse_raw_file", return_value=BytesIO(b"")),
		patch.object(irsbr_instance, "transform_data", return_value=sample_df),
		patch.object(irsbr_instance, "standardize_dataframe", return_value=sample_df),
		patch.object(irsbr_instance, "insert_table_db") as mock_insert,
	):
		result = irsbr_instance.run()
		assert result is None
		mock_insert.assert_called_once()


def test_run_all_files_fail(
	irsbr_instance: IRSBRShareholders,
) -> None:
	"""Test run method when all HTTP requests fail.

	Verifies
	--------
	- Returns empty DataFrame when every file download fails.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.

	Returns
	-------
	None
	"""
	with patch.object(
		irsbr_instance,
		"get_response",
		side_effect=requests.exceptions.HTTPError("404"),
	):
		result = irsbr_instance.run()
		assert isinstance(result, pd.DataFrame)
		assert result.empty


def test_run_iterates_over_all_files(
	irsbr_instance: IRSBRShareholders,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that run iterates over all 10 numbered files.

	Verifies
	--------
	- requests.get is called 10 times (once per file).
	- Each URL contains the correct file index.

	Parameters
	----------
	irsbr_instance : IRSBRShareholders
		Instance of IRSBRShareholders.
	mock_requests_get : object
		Mocked requests.get function.
	mock_response : Response
		Mocked Response object.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	sample_df = pd.DataFrame({"EIN_BASIC": ["12345678"]})
	with (
		patch.object(irsbr_instance, "parse_raw_file", return_value=BytesIO(b"")),
		patch.object(irsbr_instance, "transform_data", return_value=sample_df),
		patch.object(irsbr_instance, "standardize_dataframe", return_value=sample_df),
	):
		irsbr_instance.run()
		assert mock_requests_get.call_count == _NUM_FILES
		for i in range(_NUM_FILES):
			call_url = mock_requests_get.call_args_list[i][0][0]
			assert f"Socios{i}.zip" in call_url


def test_reload_module() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded without errors.
	- Instance attributes are preserved after reload.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.br.taxation.irsbr_shareholders

	original_instance = IRSBRShareholders(date_ref=date(2025, 3, 15))
	importlib.reload(stpstone.ingestion.countries.br.taxation.irsbr_shareholders)
	new_instance = IRSBRShareholders(date_ref=date(2025, 3, 15))
	assert new_instance.date_ref == original_instance.date_ref
	assert new_instance.year_month == original_instance.year_month
