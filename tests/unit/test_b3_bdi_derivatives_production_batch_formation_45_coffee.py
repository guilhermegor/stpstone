"""Unit tests for B3BdiDerivativesProductionBatchFormation45Coffee class."""

from __future__ import annotations

from datetime import date
from logging import Logger
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_production_batch_formation_45_coffee import (  # noqa: E501
	B3BdiDerivativesProductionBatchFormation45Coffee,
)
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Fixture providing a sample date.

	Returns
	-------
	date
		Sample date object.
	"""
	return date(2026, 4, 17)


@pytest.fixture
def instance(
	sample_date: date,
) -> B3BdiDerivativesProductionBatchFormation45Coffee:
	"""Fixture providing a B3BdiDerivativesProductionBatchFormation45Coffee instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	"""
	return B3BdiDerivativesProductionBatchFormation45Coffee(date_ref=sample_date)


@pytest.fixture
def sample_table_dict() -> dict:
	"""Fixture providing a sample API table dict with one data row.

	Returns
	-------
	dict
		Sample table dict mimicking the BDI DistrionCoffeLocation API response structure.
	"""
	return {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "Agio"},
			{"name": "Base"},
			{"name": "DesAgio"},
			{"name": "NonStandard"},
			{"name": "Total"},
			{"name": "Perce"},
			{"name": "BatchQuant"},
		],
		"values": [
			[
				"MG - GUAXUPE - 30028 - COOP. REG. DE C",
				0,
				0,
				106,
				0,
				106,
				28.000000000000004,
				None,
			],
		],
	}


@pytest.fixture
def empty_table_dict() -> dict:
	"""Fixture providing a table dict with empty values.

	Returns
	-------
	dict
		Sample table dict with empty values list.
	"""
	return {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "Agio"},
			{"name": "Base"},
			{"name": "DesAgio"},
			{"name": "NonStandard"},
			{"name": "Total"},
			{"name": "Perce"},
			{"name": "BatchQuant"},
		],
		"values": [],
	}


@pytest.fixture
def mock_response(sample_table_dict: dict) -> Response:
	"""Mock Response with one-row table payload.

	Parameters
	----------
	sample_table_dict : dict
		Sample table dict fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": sample_table_dict}
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_empty_response(empty_table_dict: dict) -> Response:
	"""Mock Response with empty-values table payload.

	Parameters
	----------
	empty_table_dict : dict
		Empty table dict fixture.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = {"table": empty_table_dict}
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with all parameters provided.

	Verifies
	--------
	- date_ref, int_page_size, url_tpl are set correctly.
	- Standard helpers are initialized.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesProductionBatchFormation45Coffee(
		date_ref=sample_date, int_page_size=500
	)
	assert inst.date_ref == sample_date
	assert inst.int_page_size == 500
	assert "2026-04-17" in inst.url_tpl
	assert "{page}" in inst.url_tpl
	assert "500" in inst.url_tpl
	assert "DistrionCoffeLocation" in inst.url_tpl
	assert inst.logger is None
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)


def test_init_default_page_size(sample_date: date) -> None:
	"""Test that default int_page_size is 1_000.

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesProductionBatchFormation45Coffee(date_ref=sample_date)
	assert inst.int_page_size == 1_000


def test_init_default_page_max(sample_date: date) -> None:
	"""Test that default int_page_max is 1 (single-page endpoint).

	Parameters
	----------
	sample_date : date
		Sample date for initialization.

	Returns
	-------
	None
	"""
	inst = B3BdiDerivativesProductionBatchFormation45Coffee(date_ref=sample_date)
	assert inst.int_page_max == 1


def test_init_without_date_ref(mocker: MockerFixture) -> None:
	"""Test initialization without date_ref defaults to previous working day.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch.object(DatesBRAnbima, "add_working_days", return_value=date(2026, 4, 16))
	inst = B3BdiDerivativesProductionBatchFormation45Coffee()
	assert inst.date_ref == date(2026, 4, 16)


def test_init_logger_propagated() -> None:
	"""Test that logger is stored on the instance.

	Returns
	-------
	None
	"""
	mock_logger = MagicMock(spec=Logger)
	inst = B3BdiDerivativesProductionBatchFormation45Coffee(
		date_ref=date(2026, 4, 17), logger=mock_logger
	)
	assert inst.logger is mock_logger


def test_get_response_success(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mocker: MockerFixture,
) -> None:
	"""Test get_response posts to the correct URL and returns the response.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_post = mocker.patch("requests.post", return_value=mock_resp)

	result = instance.get_response(int_page=3)

	expected_url = instance.url_tpl.format(page=3)
	mock_post.assert_called_once_with(expected_url, json={}, timeout=(12.0, 21.0), verify=True)
	assert result is mock_resp
	mock_resp.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises HTTPError on bad status.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.HTTPError("500 Server Error"))
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response()


def test_get_response_timeout_error(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mocker: MockerFixture,
) -> None:
	"""Test get_response raises Timeout when the request times out.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("requests.post", side_effect=requests.exceptions.Timeout("timed out"))
	with pytest.raises(requests.exceptions.Timeout):
		instance.get_response()


def test_parse_raw_file_returns_table(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	sample_table_dict: dict,
) -> None:
	"""Test parse_raw_file extracts the table dict from the JSON response.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	sample_table_dict : dict
		Expected table dict.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.json.return_value = {"table": sample_table_dict}
	result = instance.parse_raw_file(mock_resp)
	assert result == sample_table_dict


def test_transform_data_normal(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	sample_table_dict: dict,
) -> None:
	"""Test transform_data builds a DataFrame with UPPER_SNAKE columns.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	sample_table_dict : dict
		Sample table dict with one row.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(sample_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == [
		"TCKR_SYMB",
		"AGIO",
		"BASE",
		"DES_AGIO",
		"NON_STANDARD",
		"TOTAL",
		"PERCE",
		"BATCH_QUANT",
	]
	assert df_["TCKR_SYMB"].iloc[0] == "MG - GUAXUPE - 30028 - COOP. REG. DE C"
	assert df_["DES_AGIO"].iloc[0] == 106
	assert df_["BATCH_QUANT"].iloc[0] is None


def test_transform_data_multiple_rows(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
) -> None:
	"""Test transform_data handles multiple rows correctly.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.

	Returns
	-------
	None
	"""
	table = {
		"columns": [
			{"name": "TckrSymb"},
			{"name": "Agio"},
			{"name": "Base"},
			{"name": "DesAgio"},
			{"name": "NonStandard"},
			{"name": "Total"},
			{"name": "Perce"},
			{"name": "BatchQuant"},
		],
		"values": [
			["MG - GUAXUPE - 30028 - COOP. REG. DE C", 0, 0, 106, 0, 106, 28.0, None],
			["SP - SANTOS - 10001 - BOLSA CAFE", 10, 5, 90, 2, 107, 30.0, None],
			["TOTAL", None, None, None, None, None, None, 213],
		],
	}
	df_ = instance.transform_data(table)
	assert len(df_) == 3
	assert set(df_["TCKR_SYMB"].tolist()) == {
		"MG - GUAXUPE - 30028 - COOP. REG. DE C",
		"SP - SANTOS - 10001 - BOLSA CAFE",
		"TOTAL",
	}


def test_transform_data_empty_values(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	empty_table_dict: dict,
) -> None:
	"""Test transform_data returns empty DataFrame when values list is empty.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	empty_table_dict : dict
		Table dict with empty values.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(empty_table_dict)
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db_returns_dataframe(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run fetches pages until empty, then returns a DataFrame.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(
		instance,
		"get_response",
		side_effect=[mock_response, mock_empty_response],
	)
	mocker.patch.object(
		instance,
		"standardize_dataframe",
		side_effect=lambda df_, **kw: df_,
	)

	result = instance.run()

	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert "TCKR_SYMB" in result.columns
	assert "URL" in result.columns


def test_run_with_db(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mock_response: Response,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run inserts into DB and returns None when cls_db is set.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mock_response : Response
		Mocked Response with one data row.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch("time.sleep")
	mocker.patch.object(
		instance,
		"get_response",
		side_effect=[mock_response, mock_empty_response],
	)
	mocker.patch.object(instance, "standardize_dataframe", side_effect=lambda df_, **kw: df_)
	mock_insert = mocker.patch.object(instance, "insert_table_db")

	result = instance.run()

	assert result is None
	mock_insert.assert_called_once()


def test_run_no_data_returns_none(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mock_empty_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run returns None when the first page is already empty.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mock_empty_response : Response
		Mocked Response with empty values.
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	None
	"""
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mocker.patch.object(instance, "get_response", return_value=mock_empty_response)

	result = instance.run()

	assert result is None


@pytest.mark.parametrize(
	"timeout",
	[10, 10.5, (10.0, 20.0), (10, 20)],
)
def test_get_response_timeout_variants(
	instance: B3BdiDerivativesProductionBatchFormation45Coffee,
	mocker: MockerFixture,
	timeout: int | float | tuple,
) -> None:
	"""Test get_response accepts various timeout types.

	Parameters
	----------
	instance : B3BdiDerivativesProductionBatchFormation45Coffee
		Initialized instance.
	mocker : MockerFixture
		Pytest-mock fixture.
	timeout : int | float | tuple
		Various timeout values.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.raise_for_status = MagicMock()
	mocker.patch("backoff.on_exception", lambda *a, **kw: lambda fn: fn)
	mock_post = mocker.patch("requests.post", return_value=mock_resp)

	instance.get_response(timeout=timeout)

	_, kwargs = mock_post.call_args
	assert kwargs["timeout"] == timeout
