"""Unit tests for CvmFundsMonthlyProfile class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling
- Data parsing and transformation
- Database insertion and fallback logic
"""

from datetime import date
from io import StringIO
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.registries.cvm_funds_monthly_profile import (
	CvmFundsMonthlyProfile,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample CSV content.

	Returns
	-------
	Response
		Mocked Response object with predefined content.
	"""
	response = MagicMock(spec=Response)
	response.content = (
		b"TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC;VERSAO;NR_COTST_PF_PB\n"
		b"CLASSES - FIF;00.017.024/0001-53;FIF - CLASSE RENDA FIXA;2026-01-31;4;0\n"
		b"CLASSES - FIF;00.068.305/0001-35;CAIXA EMPREENDER FIC;2026-01-31;4;9\n"
	)
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


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
def mock_sleep(mocker: MockerFixture) -> object:
	"""Mock time.sleep to eliminate delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks.

	Returns
	-------
	object
		Mocked time.sleep object.
	"""
	return mocker.patch("time.sleep")


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
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		Fixed date for consistent testing.
	"""
	return date(2026, 1, 1)


@pytest.fixture
def cvm_instance(sample_date: date) -> CvmFundsMonthlyProfile:
	"""Fixture providing CvmFundsMonthlyProfile instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	CvmFundsMonthlyProfile
		Initialized CvmFundsMonthlyProfile instance.
	"""
	return CvmFundsMonthlyProfile(date_ref=sample_date)


@pytest.fixture
def sample_file() -> StringIO:
	"""Provide sample CSV file content for parsing.

	Returns
	-------
	StringIO
		Sample file content.
	"""
	content = (
		"TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC;VERSAO;NR_COTST_PF_PB\n"
		"CLASSES - FIF;00.017.024/0001-53;FIF - CLASSE RENDA FIXA;2026-01-31;4;0\n"
		"CLASSES - FIF;00.068.305/0001-35;CAIXA EMPREENDER FIC;2026-01-31;4;9\n"
	)
	return StringIO(content)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- The CvmFundsMonthlyProfile instance is initialized correctly.
	- Attributes are set as expected.
	- Date formatting works correctly.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	instance = CvmFundsMonthlyProfile(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert instance.date_ref_yyyymm == "202601"
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert "perfil_mensal_fi_202601.csv" in instance.url


def test_init_with_default_date() -> None:
	"""Test initialization with default date.

	Verifies
	--------
	- Default date is set to first day of the previous calendar month.
	- URL is correctly formatted.

	Returns
	-------
	None
	"""
	with patch.object(DatesCurrent, "curr_date", return_value=date(2026, 3, 15)):
		instance = CvmFundsMonthlyProfile()
		assert instance.date_ref == date(2026, 2, 1)
		assert instance.date_ref_yyyymm == "202602"
		assert instance.url.endswith("perfil_mensal_fi_202602.csv")


def test_get_response_success(
	cvm_instance: CvmFundsMonthlyProfile,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	--------
	- Successful HTTP request returns Response object.
	- Correct parameters are passed to requests.get.
	- No retries are attempted on success.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
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
	result = cvm_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(cvm_instance.url, timeout=(12.0, 21.0), verify=True)
	mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(cvm_instance: CvmFundsMonthlyProfile, mock_response: Response) -> None:
	"""Test parsing of raw file content.

	Verifies
	--------
	- Response content is correctly parsed into StringIO.
	- get_file method is called correctly.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
	mock_response : Response
		Mocked Response object.

	Returns
	-------
	None
	"""
	with patch.object(
		cvm_instance, "get_file", return_value=StringIO("test content")
	) as mock_get_file:
		result = cvm_instance.parse_raw_file(mock_response)
		assert isinstance(result, StringIO)
		mock_get_file.assert_called_once_with(resp_req=mock_response)


def test_transform_data(cvm_instance: CvmFundsMonthlyProfile, sample_file: StringIO) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	--------
	- Input file is correctly transformed into DataFrame.
	- Column names are present.
	- Row count matches input.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
	sample_file : StringIO
		Sample file content.

	Returns
	-------
	None
	"""
	df_ = cvm_instance.transform_data(sample_file)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty, "DataFrame should not be empty"
	assert "TP_FUNDO_CLASSE" in df_.columns
	assert "CNPJ_FUNDO_CLASSE" in df_.columns
	assert "DT_COMPTC" in df_.columns
	assert "VERSAO" in df_.columns
	assert "NR_COTST_PF_PB" in df_.columns
	assert len(df_) == 2
	assert df_["TP_FUNDO_CLASSE"].iloc[0] == "CLASSES - FIF"
	assert df_["NR_COTST_PF_PB"].iloc[1] == 9


def test_run_without_db(
	cvm_instance: CvmFundsMonthlyProfile,
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
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
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
	with (
		patch.object(cvm_instance, "parse_raw_file", return_value=StringIO("test")) as mock_parse,
		patch.object(
			cvm_instance, "transform_data", return_value=pd.DataFrame({"VERSAO": [4]})
		) as mock_transform,
		patch.object(
			cvm_instance, "standardize_dataframe", return_value=pd.DataFrame({"VERSAO": [4]})
		) as mock_standardize,
	):
		result = cvm_instance.run()
		assert isinstance(result, pd.DataFrame)
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()


def test_run_with_db(
	cvm_instance: CvmFundsMonthlyProfile,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Database insertion is called when cls_db is provided.
	- No DataFrame is returned.
	- All intermediate methods are called correctly.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
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
	cvm_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with (
		patch.object(cvm_instance, "parse_raw_file", return_value=StringIO("test")) as mock_parse,
		patch.object(
			cvm_instance, "transform_data", return_value=pd.DataFrame({"VERSAO": [4]})
		) as mock_transform,
		patch.object(
			cvm_instance, "standardize_dataframe", return_value=pd.DataFrame({"VERSAO": [4]})
		) as mock_standardize,
		patch.object(cvm_instance, "insert_table_db") as mock_insert,
	):
		result = cvm_instance.run()
		assert result is None
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()
		mock_insert.assert_called_once()


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
	cvm_instance: CvmFundsMonthlyProfile,
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
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
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
	result = cvm_instance.get_response(timeout=timeout)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(cvm_instance.url, timeout=timeout, verify=True)


@pytest.mark.parametrize(
	"invalid_input,expected_error",
	[
		(None, "resp_req must be one of types: Response, Page, WebDriver, got NoneType"),
		("invalid", "resp_req must be one of types: Response, Page, WebDriver, got str"),
		(123, "resp_req must be one of types: Response, Page, WebDriver, got int"),
		([], "resp_req must be one of types: Response, Page, WebDriver, got list"),
	],
)
def test_parse_raw_file_invalid_input(
	cvm_instance: CvmFundsMonthlyProfile,
	invalid_input: Union[None, str, int, list],
	expected_error: str,
) -> None:
	"""Test parse_raw_file with invalid inputs.

	Verifies
	--------
	- Invalid response types raise appropriate exceptions.
	- Type checker enforces correct input types.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.
	invalid_input : Union[None, str, int, list]
		Invalid input to test error handling.
	expected_error : str
		Expected error message from type checker.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match=expected_error):
		cvm_instance.parse_raw_file(invalid_input)


def test_transform_data_empty_file(cvm_instance: CvmFundsMonthlyProfile) -> None:
	"""Test transform_data with empty file.

	Verifies
	--------
	- Empty file input results in empty DataFrame.
	- No errors are raised for empty input.

	Parameters
	----------
	cvm_instance : CvmFundsMonthlyProfile
		Instance of CvmFundsMonthlyProfile.

	Returns
	-------
	None
	"""
	empty_file = StringIO("")
	df_ = cvm_instance.transform_data(empty_file)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


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

	import stpstone.ingestion.countries.br.registries.cvm_funds_monthly_profile

	original_instance = CvmFundsMonthlyProfile(date_ref=date(2026, 1, 1))
	importlib.reload(stpstone.ingestion.countries.br.registries.cvm_funds_monthly_profile)
	new_instance = CvmFundsMonthlyProfile(date_ref=date(2026, 1, 1))
	assert new_instance.date_ref == original_instance.date_ref
	assert new_instance.url == original_instance.url
